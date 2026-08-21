#![cfg_attr(windows, windows_subsystem = "windows")]

//! Local control plane for the native Windows UI.
//! The GUI never owns configuration parsing or agent processes directly.

use std::{
    env, fs,
    io::{BufRead, BufReader, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{mpsc, Arc, Mutex},
    thread,
    time::{Duration, Instant},
};

#[cfg(windows)]
use std::os::windows::{io::AsRawHandle, process::CommandExt};

use anyhow::{anyhow, bail, Context, Result};
use clap::{Parser, ValueEnum};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader as AsyncBufReader},
    process::Command as AsyncCommand,
    sync::broadcast,
    time::timeout,
};
use uuid::Uuid;
use wechat_summary_core::AgentConfig;

const PROTOCOL_VERSION: u32 = 1;
const CREATE_NO_WINDOW: u32 = 0x0800_0000;
const MAX_REQUEST_BYTES: usize = 2 * 1024 * 1024;
/// How long to wait for the user to accept the UAC prompt before reporting a
/// failed operation. Once the elevated process reports "running", there is no
/// upper bound: long installs are legitimate.
const ELEVATION_CONFIRM_TIMEOUT: Duration = Duration::from_secs(300);

// Elevated operations must never execute scripts or binaries resolved from the
// (user-writable) install directory. The runtime installer is compiled into this
// binary so an attacker with write access to the install root cannot swap it for
// a payload that would run as administrator.
const EMBEDDED_RUNTIME_INSTALL_SCRIPT: &str =
    include_str!("../../../../scripts/install-python-runtime.ps1");

#[derive(Debug, Parser)]
#[command(name = "wechat-summary-control")]
#[command(about = "Local control service for SummaryAgent4GroupChat")]
struct Args {
    #[arg(long, default_value = r"\\.\pipe\SummaryAgent4GroupChat.Control.v1")]
    pipe: String,
    /// Shared secret for authorizing control requests. Prefer passing it via
    /// the SUMMARY_AGENT_CONTROL_TOKEN environment variable: command lines are
    /// visible to every local process through Win32 process introspection.
    #[arg(long)]
    token: Option<String>,
    #[arg(long)]
    config: Option<PathBuf>,
    #[arg(long)]
    working_dir: Option<PathBuf>,
    #[arg(long, value_enum)]
    elevated: Option<ElevatedOperation>,
    #[arg(long)]
    operation_id: Option<String>,
    #[arg(long)]
    update_package: Option<String>,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum ElevatedOperation {
    RuntimeInstall,
    WxdbInit,
    WxdbUpdate,
    PipUpdate,
    ApplicationUpdate,
}

impl ElevatedOperation {
    fn name(self) -> &'static str {
        match self {
            Self::RuntimeInstall => "runtime.install",
            Self::WxdbInit => "wxdb.init",
            Self::WxdbUpdate => "wxdb.update",
            Self::PipUpdate => "pip.update",
            Self::ApplicationUpdate => "application.update",
        }
    }
}

#[derive(Clone)]
struct AppPaths {
    config_path: PathBuf,
    working_dir: PathBuf,
}

struct ManagedAgent {
    child: Child,
    pid: u32,
    #[cfg(windows)]
    _job: Job,
}

#[cfg(windows)]
struct Job(windows_sys::Win32::Foundation::HANDLE);

#[cfg(windows)]
unsafe impl Send for Job {}
#[cfg(windows)]
unsafe impl Sync for Job {}

#[cfg(windows)]
impl Drop for Job {
    fn drop(&mut self) {
        unsafe {
            windows_sys::Win32::Foundation::CloseHandle(self.0);
        }
    }
}

#[derive(Clone)]
struct ControlState {
    paths: AppPaths,
    agent: Arc<Mutex<Option<ManagedAgent>>>,
    events: broadcast::Sender<Event>,
}

#[derive(Debug, Clone, Serialize)]
struct Event {
    version: u32,
    event: String,
    data: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OperationStatus {
    operation: String,
    state: String,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    success: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct Request {
    version: u32,
    id: Value,
    method: String,
    token: String,
    #[serde(default)]
    params: Value,
}

#[derive(Serialize)]
struct Response {
    version: u32,
    id: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<ApiError>,
}

#[derive(Debug, Serialize)]
struct ApiError {
    code: String,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    detail: Option<String>,
    retryable: bool,
}

impl ApiError {
    fn invalid(message: impl Into<String>) -> Self {
        Self {
            code: "invalid_request".into(),
            message: message.into(),
            detail: None,
            retryable: false,
        }
    }

    fn internal(error: impl std::fmt::Display) -> Self {
        Self {
            code: "internal_error".into(),
            message: "操作失败".into(),
            detail: Some(redact_secret_like_tokens(&error.to_string())),
            retryable: false,
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let paths = resolve_paths(args.config.as_deref(), args.working_dir.as_deref())?;

    if let Some(operation) = args.elevated {
        return run_elevated(
            operation,
            &paths,
            args.operation_id.as_deref(),
            args.update_package.as_deref(),
        );
    }

    let token = args
        .token
        .or_else(|| env::var("SUMMARY_AGENT_CONTROL_TOKEN").ok())
        .map(|token| token.trim().to_string())
        .filter(|token| !token.is_empty())
        .ok_or_else(|| {
            anyhow!(
                "--token or the SUMMARY_AGENT_CONTROL_TOKEN environment variable is required \
                 for the control service"
            )
        })?;
    let (events, _) = broadcast::channel(512);
    let state = ControlState {
        paths,
        agent: Arc::new(Mutex::new(None)),
        events,
    };
    emit(&state, "status.changed", status_payload(&state)?);
    run_server(args.pipe, token, state).await
}

fn resolve_paths(config_arg: Option<&Path>, working_arg: Option<&Path>) -> Result<AppPaths> {
    let requested = config_arg.unwrap_or_else(|| Path::new("config/agent.toml"));
    let config_path = if requested.is_absolute() {
        requested.to_path_buf()
    } else {
        let cwd_candidate = env::current_dir()?.join(requested);
        if cwd_candidate.exists() {
            cwd_candidate
        } else if let Some(exe_dir) = env::current_exe()?.parent() {
            let root = if exe_dir.file_name().is_some_and(|name| name == "bin") {
                exe_dir.parent().unwrap_or(exe_dir)
            } else {
                exe_dir
            };
            root.join(requested)
        } else {
            cwd_candidate
        }
    };
    if !config_path.exists() {
        bail!("config file does not exist: {}", config_path.display());
    }
    let working_dir = working_arg.map(Path::to_path_buf).unwrap_or_else(|| {
        config_path
            .parent()
            .and_then(Path::parent)
            .filter(|_| {
                config_path
                    .parent()
                    .and_then(Path::file_name)
                    .is_some_and(|name| name == "config")
            })
            .map(Path::to_path_buf)
            .unwrap_or_else(|| env::current_dir().unwrap_or_default())
    });
    Ok(AppPaths {
        config_path,
        working_dir,
    })
}

#[cfg(windows)]
async fn run_server(pipe: String, token: String, state: ControlState) -> Result<()> {
    use std::{ffi::c_void, ptr};
    use tokio::net::windows::named_pipe::{NamedPipeServer, ServerOptions};
    use windows_sys::Win32::{
        Foundation::LocalFree,
        Security::{
            Authorization::ConvertStringSecurityDescriptorToSecurityDescriptorW,
            SECURITY_ATTRIBUTES,
        },
    };

    fn create_server(pipe: &str, first: bool) -> Result<NamedPipeServer> {
        let descriptor: Vec<u16> = "D:P(A;;GA;;;SY)(A;;GA;;;BA)(A;;GA;;;OW)"
            .encode_utf16()
            .chain(Some(0))
            .collect();
        let mut security_descriptor = ptr::null_mut();
        let ok = unsafe {
            ConvertStringSecurityDescriptorToSecurityDescriptorW(
                descriptor.as_ptr(),
                1,
                &mut security_descriptor,
                ptr::null_mut(),
            )
        };
        if ok == 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        let mut attrs = SECURITY_ATTRIBUTES {
            nLength: std::mem::size_of::<SECURITY_ATTRIBUTES>() as u32,
            lpSecurityDescriptor: security_descriptor,
            bInheritHandle: 0,
        };
        let mut options = ServerOptions::new();
        options.reject_remote_clients(true);
        if first {
            options.first_pipe_instance(true);
        }
        let created = unsafe {
            options.create_with_security_attributes_raw(pipe, &mut attrs as *mut _ as *mut c_void)
        };
        unsafe {
            LocalFree(security_descriptor);
        }
        Ok(created?)
    }

    let mut server = create_server(&pipe, true)?;
    loop {
        let next = create_server(&pipe, false)?;
        server
            .connect()
            .await
            .context("waiting for local control client")?;
        let connected = std::mem::replace(&mut server, next);
        let state = state.clone();
        let token = token.clone();
        tokio::spawn(async move {
            let _ = serve_connection(connected, state, token).await;
        });
    }
}

#[cfg(not(windows))]
async fn run_server(_pipe: String, _token: String, _state: ControlState) -> Result<()> {
    bail!("the local control service is supported on Windows only")
}

#[cfg(windows)]
async fn serve_connection(
    pipe: tokio::net::windows::named_pipe::NamedPipeServer,
    state: ControlState,
    token: String,
) -> Result<()> {
    let mut reader = AsyncBufReader::new(pipe);
    let mut line = String::new();
    let count = reader.read_line(&mut line).await?;
    if count == 0 || count > MAX_REQUEST_BYTES {
        return Ok(());
    }
    let request: Request = match serde_json::from_str(&line) {
        Ok(request) => request,
        Err(error) => {
            let mut pipe = reader.into_inner();
            write_response(
                &mut pipe,
                Response {
                    version: PROTOCOL_VERSION,
                    id: Value::Null,
                    result: None,
                    error: Some(ApiError::invalid(format!("invalid JSON: {error}"))),
                },
            )
            .await?;
            return Ok(());
        }
    };
    let mut pipe = reader.into_inner();
    if request.version != PROTOCOL_VERSION || !tokens_match(&request.token, &token) {
        write_response(
            &mut pipe,
            Response {
                version: PROTOCOL_VERSION,
                id: request.id,
                result: None,
                error: Some(ApiError {
                    code: "unauthorized".into(),
                    message: "控制连接未获授权".into(),
                    detail: None,
                    retryable: false,
                }),
            },
        )
        .await?;
        return Ok(());
    }
    let is_subscription = matches!(
        request.method.as_str(),
        "output.subscribe" | "logs.subscribe" | "status.subscribe" | "operation.subscribe"
    );
    let response = match dispatch(&state, &request.method, &request.params).await {
        Ok(result) => Response {
            version: PROTOCOL_VERSION,
            id: request.id,
            result: Some(result),
            error: None,
        },
        Err(error) => Response {
            version: PROTOCOL_VERSION,
            id: request.id,
            result: None,
            error: Some(error),
        },
    };
    write_response(&mut pipe, response).await?;
    if !is_subscription {
        return Ok(());
    }
    let topic = request.method.trim_end_matches(".subscribe").to_string();
    let mut receiver = state.events.subscribe();
    loop {
        match receiver.recv().await {
            Ok(event)
                if (topic == "output" && event.event == "output")
                    || (topic == "logs" && event.event == "log")
                    || (topic == "status" && event.event == "status.changed")
                    || (topic == "operation" && event.event.starts_with("operation.")) =>
            {
                write_event(&mut pipe, event).await?;
            }
            Ok(_) => {}
            Err(broadcast::error::RecvError::Lagged(_)) => {}
            Err(broadcast::error::RecvError::Closed) => return Ok(()),
        }
    }
}

#[cfg(windows)]
async fn write_response(
    pipe: &mut tokio::net::windows::named_pipe::NamedPipeServer,
    response: Response,
) -> Result<()> {
    let text = serde_json::to_string(&response)?;
    pipe.write_all(text.as_bytes()).await?;
    pipe.write_all(b"\n").await?;
    pipe.flush().await?;
    Ok(())
}

#[cfg(windows)]
async fn write_event(
    pipe: &mut tokio::net::windows::named_pipe::NamedPipeServer,
    event: Event,
) -> Result<()> {
    let text = serde_json::to_string(&event)?;
    pipe.write_all(text.as_bytes()).await?;
    pipe.write_all(b"\n").await?;
    pipe.flush().await?;
    Ok(())
}

async fn dispatch(
    state: &ControlState,
    method: &str,
    params: &Value,
) -> std::result::Result<Value, ApiError> {
    let result = match method {
        "status.get" => status_payload(state),
        "config.read" => config_read(state),
        "config.validate" => config_validate(params),
        "config.write" => config_write(state, params),
        "config.patch" => config_patch(state, params),
        "agent.start" => agent_start(state),
        "agent.stop" => agent_stop(state),
        "runtime.install" => start_elevated_operation(state, ElevatedOperation::RuntimeInstall),
        "wxdb.init" => start_elevated_operation(state, ElevatedOperation::WxdbInit),
        "update.install" => start_update_install(state, params),
        "runtime.check" => runtime_check(state).await,
        "path.open" => path_open(state, params),
        "logs.tail" => logs_tail(state),
        "update.check" => update_check(state).await,
        "output.subscribe" | "logs.subscribe" | "status.subscribe" | "operation.subscribe" => {
            Ok(json!({ "subscribed": true }))
        }
        _ => {
            return Err(ApiError::invalid(format!(
                "unsupported control method: {method}"
            )))
        }
    };
    result.map_err(ApiError::internal)
}

fn status_payload(state: &ControlState) -> Result<Value> {
    let config = AgentConfig::from_path(&state.paths.config_path)
        .context("validating current configuration")?;
    let targets = if config.platform.kind.as_str() == "discord" {
        config.discord.channels.len()
    } else {
        config.wx4py.groups.len()
    };
    let running = state
        .agent
        .lock()
        .map_err(|_| anyhow!("agent lock poisoned"))?
        .as_ref()
        .map(|agent| agent.pid);
    Ok(json!({
        "agent_running": running.is_some(),
        "agent_pid": running,
        "platform": config.platform.kind.as_str(),
        "targets": targets,
        "config_path": state.paths.config_path,
        "working_dir": state.paths.working_dir,
        "llm_configured": configured_value(&config.llm.api_key, &config.llm.api_key_env),
        "image_configured": configured_value(&config.image_gen.api_key, &config.image_gen.api_key_env),
        "wxdb_configured": !config.wx_cli.executable.trim().is_empty(),
    }))
}

fn configured_value(value: &Option<String>, env_name: &str) -> bool {
    value
        .as_deref()
        .is_some_and(|value| !value.trim().is_empty())
        || env::var(env_name).is_ok_and(|value| !value.trim().is_empty())
}

fn config_read(state: &ControlState) -> Result<Value> {
    let raw = fs::read_to_string(&state.paths.config_path)?;
    let validation = AgentConfig::from_toml_str(&raw)
        .map(|_| String::new())
        .unwrap_or_else(|error| error.to_string());
    // Structured view derived from the REDACTED document so secrets never
    // reach the UI; clients edit through config.patch, which applies values
    // server-side without ever echoing secrets back.
    let parsed = redact_toml_secrets(&raw)?
        .parse::<toml::Value>()
        .ok()
        .and_then(|value| serde_json::to_value(value).ok())
        .unwrap_or(Value::Null);
    Ok(
        json!({ "toml": redact_toml_secrets(&raw)?, "validation": validation, "path": state.paths.config_path, "parsed": parsed }),
    )
}

fn config_validate(params: &Value) -> Result<Value> {
    let text = required_toml(params)?;
    match AgentConfig::from_toml_str(text) {
        Ok(_) => Ok(json!({ "valid": true, "message": "配置有效" })),
        Err(error) => Ok(json!({ "valid": false, "message": error.to_string() })),
    }
}

fn config_write(state: &ControlState, params: &Value) -> Result<Value> {
    let submitted = required_toml(params)?;
    let previous = fs::read_to_string(&state.paths.config_path)?;
    let mut new_value: toml::Value = submitted.parse().context("parsing submitted TOML")?;
    let old_value: toml::Value = previous.parse().context("parsing existing TOML")?;
    preserve_redacted_secrets(&mut new_value, &old_value, None);
    let normalized = toml::to_string_pretty(&new_value)?;
    AgentConfig::from_toml_str(&normalized).context("validating submitted TOML")?;
    atomic_write(&state.paths.config_path, normalized.as_bytes())?;
    emit(
        state,
        "config.reloaded",
        json!({ "path": state.paths.config_path }),
    );
    emit(state, "status.changed", status_payload(state)?);
    Ok(json!({ "saved": true }))
}

fn required_toml(params: &Value) -> Result<&str> {
    params
        .get("toml")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("params.toml must be a string"))
}

#[derive(Debug, serde::Deserialize)]
struct ConfigPatchOperation {
    /// Table path, e.g. ["llm"] or ["room_capabilities"].
    section: Vec<String>,
    key: String,
    /// JSON value to set; null removes the key.
    value: Value,
}

#[derive(Debug, serde::Deserialize)]
struct ConfigPatchParams {
    operations: Vec<ConfigPatchOperation>,
}

fn config_patch(state: &ControlState, params: &Value) -> Result<Value> {
    let params: ConfigPatchParams =
        serde_json::from_value(params.clone()).context("invalid config.patch parameters")?;
    let raw = fs::read_to_string(&state.paths.config_path)?;
    let patched = apply_config_patch(&raw, &params)?;
    AgentConfig::from_toml_str(&patched).context("patched configuration failed validation")?;
    atomic_write(&state.paths.config_path, patched.as_bytes())?;
    emit(
        state,
        "config.reloaded",
        json!({ "path": state.paths.config_path }),
    );
    emit(state, "status.changed", status_payload(state)?);
    Ok(json!({ "patched": true, "operations": params.operations.len() }))
}

/// Format-preserving TOML edit driven by key-level operations. Pure so it can
/// be unit tested without a ControlState.
fn apply_config_patch(raw: &str, params: &ConfigPatchParams) -> Result<String> {
    let mut document = raw
        .parse::<toml_edit::DocumentMut>()
        .context("parsing current config TOML")?;
    for operation in &params.operations {
        if operation.section.is_empty() {
            bail!("patch operation section must not be empty");
        }
        if operation.key.trim().is_empty() {
            bail!("patch operation key must not be empty");
        }
        let table = navigate_to_table(document.as_table_mut(), &operation.section)?;
        if operation.value.is_null() {
            table.remove(&operation.key);
        } else if let Some(toml_edit::Item::Value(existing)) = table.get_mut(&operation.key) {
            // Update in place so trailing comments and surrounding whitespace
            // (the item's decor) survive the edit.
            let prefix = existing
                .decor()
                .prefix()
                .and_then(toml_edit::RawString::as_str)
                .map(std::string::ToString::to_string);
            let suffix = existing
                .decor()
                .suffix()
                .and_then(toml_edit::RawString::as_str)
                .map(std::string::ToString::to_string);
            let mut replacement = json_to_toml_value(&operation.value)?;
            if let Some(prefix) = prefix {
                replacement.decor_mut().set_prefix(prefix);
            }
            if let Some(suffix) = suffix {
                replacement.decor_mut().set_suffix(suffix);
            }
            *existing = replacement;
        } else {
            table.insert(&operation.key, json_to_toml_item(&operation.value)?);
        }
    }
    Ok(document.to_string())
}

fn navigate_to_table<'a>(
    root: &'a mut toml_edit::Table,
    section: &[String],
) -> Result<&'a mut toml_edit::Table> {
    let mut cursor = root;
    for part in section {
        cursor = cursor
            .entry(part)
            .or_insert(toml_edit::Item::Table(toml_edit::Table::new()))
            .as_table_mut()
            .ok_or_else(|| anyhow!("config [{}] conflicts with a non-table value", part))?;
    }
    Ok(cursor)
}

fn json_to_toml_item(value: &Value) -> Result<toml_edit::Item> {
    Ok(toml_edit::Item::Value(json_to_toml_value(value)?))
}

fn json_to_toml_value(value: &Value) -> Result<toml_edit::Value> {
    let item = match value {
        Value::String(text) => toml_edit::Value::from(text.clone()),
        Value::Bool(flag) => toml_edit::Value::from(*flag),
        Value::Number(number) => {
            if let Some(integer) = number.as_i64() {
                toml_edit::Value::from(integer)
            } else {
                toml_edit::Value::from(number.as_f64().context("unsupported number value")?)
            }
        }
        Value::Array(items) => {
            let mut array = toml_edit::Array::new();
            for item in items {
                match item {
                    Value::String(text) => array.push(text.clone()),
                    Value::Number(_) | Value::Bool(_) => {
                        array.push(json_scalar_to_toml(item)?);
                    }
                    other => bail!("unsupported array element in patch value: {other}"),
                }
            }
            toml_edit::Value::Array(array)
        }
        Value::Object(fields) => {
            let mut inline = toml_edit::InlineTable::new();
            for (name, field) in fields {
                inline.insert(name, json_scalar_to_toml(field)?);
            }
            toml_edit::Value::InlineTable(inline)
        }
        other => bail!("unsupported patch value: {other}"),
    };
    Ok(item)
}

fn json_scalar_to_toml(value: &Value) -> Result<toml_edit::Value> {
    match value {
        Value::String(text) => Ok(toml_edit::Value::from(text.clone())),
        Value::Bool(flag) => Ok(toml_edit::Value::from(*flag)),
        Value::Number(number) => {
            if let Some(integer) = number.as_i64() {
                Ok(toml_edit::Value::from(integer))
            } else {
                number
                    .as_f64()
                    .map(toml_edit::Value::from)
                    .context("unsupported number value")
            }
        }
        other => bail!("unsupported scalar in patch value: {other}"),
    }
}

fn preserve_redacted_secrets(
    new_value: &mut toml::Value,
    old_value: &toml::Value,
    key: Option<&str>,
) {
    match (new_value, old_value) {
        (toml::Value::String(new_text), old)
            if key.is_some_and(is_secret_key) && new_text.starts_with("<redacted") =>
        {
            *new_text = old.as_str().unwrap_or_default().to_string()
        }
        (toml::Value::Array(new_items), toml::Value::Array(old_items)) => {
            for (index, item) in new_items.iter_mut().enumerate() {
                if let Some(old_item) = old_items.get(index) {
                    preserve_redacted_secrets(item, old_item, key);
                }
            }
        }
        (toml::Value::Table(new_table), toml::Value::Table(old_table)) => {
            for (name, value) in new_table.iter_mut() {
                if let Some(old) = old_table.get(name) {
                    preserve_redacted_secrets(value, old, Some(name));
                }
            }
        }
        _ => {}
    }
}

fn redact_toml_secrets(text: &str) -> Result<String> {
    let mut value: toml::Value = text.parse()?;
    redact_value_secrets(&mut value, None);
    Ok(toml::to_string_pretty(&value)?)
}

fn redact_value_secrets(value: &mut toml::Value, key: Option<&str>) {
    match value {
        toml::Value::String(text) if key.is_some_and(is_secret_key) && !text.trim().is_empty() => {
            *text = "<redacted-secret>".to_string()
        }
        toml::Value::Array(items) => {
            for item in items {
                redact_value_secrets(item, key);
            }
        }
        toml::Value::Table(table) => {
            for (name, item) in table {
                redact_value_secrets(item, Some(name));
            }
        }
        _ => {}
    }
}

fn is_secret_key(key: &str) -> bool {
    let key = key.to_ascii_lowercase();
    // Environment variable *names* (e.g. api_key_env) are not secrets.
    if key.ends_with("_env") {
        return false;
    }
    key == "token"
        || key == "api_key"
        || key == "api_keys"
        || key.contains("secret")
        || key.contains("password")
}

fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("config path has no parent"))?;
    let temp = parent.join(format!(
        ".{}.{}.tmp",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("agent.toml"),
        Uuid::new_v4()
    ));
    fs::write(&temp, bytes)?;
    fs::rename(&temp, path).or_else(|_| {
        fs::copy(&temp, path)?;
        fs::remove_file(&temp)
    })?;
    Ok(())
}

fn agent_start(state: &ControlState) -> Result<Value> {
    let mut guard = state
        .agent
        .lock()
        .map_err(|_| anyhow!("agent lock poisoned"))?;
    if let Some(agent) = guard.as_mut() {
        if agent.child.try_wait()?.is_none() {
            return Ok(json!({ "started": false, "pid": agent.pid, "message": "主程序已经运行" }));
        }
    }
    *guard = None;
    let executable = find_program(&state.paths, "wechat-summary-app")?;
    let mut command = Command::new(executable);
    command
        .arg("--config")
        .arg(&state.paths.config_path)
        .current_dir(&state.paths.working_dir)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    hide_window(&mut command);
    let mut child = command.spawn().context("starting wechat-summary-app")?;
    let pid = child.id();
    let stdout = child.stdout.take().context("capturing agent stdout")?;
    let stderr = child.stderr.take().context("capturing agent stderr")?;
    spawn_output_reader(state.events.clone(), stdout, "stdout");
    spawn_output_reader(state.events.clone(), stderr, "stderr");
    #[cfg(windows)]
    let job = Job::assign(&child)?;
    *guard = Some(ManagedAgent {
        child,
        pid,
        #[cfg(windows)]
        _job: job,
    });
    drop(guard);
    emit(state, "agent.started", json!({ "pid": pid }));
    emit(state, "status.changed", status_payload(state)?);
    Ok(json!({ "started": true, "pid": pid }))
}

fn agent_stop(state: &ControlState) -> Result<Value> {
    let mut guard = state
        .agent
        .lock()
        .map_err(|_| anyhow!("agent lock poisoned"))?;
    let Some(mut agent) = guard.take() else {
        return Ok(json!({ "stopped": false, "message": "主程序未由控制层托管" }));
    };
    let pid = agent.pid;
    let _ = agent.child.kill();
    let _ = agent.child.wait();
    drop(agent);
    drop(guard);
    emit(
        state,
        "agent.exited",
        json!({ "pid": pid, "reason": "stopped" }),
    );
    emit(state, "status.changed", status_payload(state)?);
    Ok(json!({ "stopped": true, "pid": pid }))
}

fn start_elevated_operation(state: &ControlState, operation: ElevatedOperation) -> Result<Value> {
    start_elevated_operation_with_package(state, operation, None)
}

fn start_update_install(state: &ControlState, params: &Value) -> Result<Value> {
    let target = params
        .get("target")
        .and_then(Value::as_str)
        .unwrap_or_default();
    match target {
        "application" => {
            start_elevated_operation_with_package(state, ElevatedOperation::ApplicationUpdate, None)
        }
        "wxdb" => start_elevated_operation_with_package(state, ElevatedOperation::WxdbUpdate, None),
        "pip" => {
            let package = params
                .get("package")
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow!("pip update requires a package name"))?
                .trim();
            if !is_safe_pip_package_name(package) {
                bail!("invalid pip package name");
            }
            start_elevated_operation_with_package(
                state,
                ElevatedOperation::PipUpdate,
                Some(package),
            )
        }
        _ => bail!("unsupported update target"),
    }
}

fn start_elevated_operation_with_package(
    state: &ControlState,
    operation: ElevatedOperation,
    update_package: Option<&str>,
) -> Result<Value> {
    let id = Uuid::new_v4().to_string();
    let (log_path, status_path) = operation_paths(&state.paths, &id)?;
    fs::File::create(&log_path)?;
    write_operation_status(
        &status_path,
        OperationStatus {
            operation: operation.name().into(),
            state: "pending".into(),
            message: "已请求管理员权限，等待确认。".into(),
            success: None,
        },
    )?;
    spawn_operation_monitor(
        state.clone(),
        id.clone(),
        operation.name().into(),
        log_path,
        status_path,
    );
    let executable = env::current_exe()?;
    let mut arguments = format!(
        "--elevated {} --config \"{}\" --working-dir \"{}\" --operation-id {}",
        operation.to_possible_value().expect("value").get_name(),
        state.paths.config_path.display(),
        state.paths.working_dir.display(),
        id
    );
    if let Some(package) = update_package {
        arguments.push_str(&format!(" --update-package {package}"));
    }
    let mut command = Command::new("powershell.exe");
    command.args([
        "-NoProfile",
        "-NonInteractive",
        "-Command",
        &format!(
            "Start-Process -FilePath '{}' -ArgumentList '{}' -Verb RunAs -WindowStyle Hidden",
            ps_quote(&executable.display().to_string()),
            ps_quote(&arguments)
        ),
    ]);
    hide_window(&mut command);
    if let Err(error) = command
        .spawn()
        .context("requesting elevation for approved operation")
    {
        let (_, status_path) = operation_paths(&state.paths, &id)?;
        let message = redact_secret_like_tokens(&error.to_string());
        let _ = write_operation_status(
            &status_path,
            OperationStatus {
                operation: operation.name().into(),
                state: "failed".into(),
                message: message.clone(),
                success: Some(false),
            },
        );
        return Err(anyhow!(message));
    }
    emit(
        state,
        "operation.progress",
        json!({ "id": id, "operation": operation.name(), "message": "已请求管理员权限，等待确认。" }),
    );
    Ok(json!({ "operation_id": id, "operation": operation.name(), "elevation_requested": true }))
}

fn run_elevated(
    operation: ElevatedOperation,
    paths: &AppPaths,
    operation_id: Option<&str>,
    update_package: Option<&str>,
) -> Result<()> {
    let id = operation_id.unwrap_or("manual");
    let (log_path, status_path) = operation_paths(paths, id)?;
    write_operation_status(
        &status_path,
        OperationStatus {
            operation: operation.name().into(),
            state: "running".into(),
            message: "管理员权限已确认，正在执行。".into(),
            success: None,
        },
    )?;
    let result = match operation {
        ElevatedOperation::RuntimeInstall => {
            let script =
                materialize_embedded_script("runtime-install", EMBEDDED_RUNTIME_INSTALL_SCRIPT)?;
            run_logged(
                Command::new("powershell.exe")
                    .args(["-NoProfile", "-ExecutionPolicy", "Bypass", "-File"])
                    .arg(&script)
                    .arg("-RootPath")
                    .arg(&paths.working_dir)
                    .arg("-ConfigPath")
                    .arg(&paths.config_path),
                &log_path,
            )
        }
        ElevatedOperation::WxdbInit => {
            let config = AgentConfig::from_path(&paths.config_path)?;
            let executable = config.wx_cli.executable;
            if executable.trim().is_empty() {
                bail!("wxdb executable is not configured");
            }
            let mut command = Command::new(executable);
            command.arg("init").current_dir(&paths.working_dir);
            run_logged(&mut command, &log_path)
        }
        ElevatedOperation::WxdbUpdate => run_wxdb_update(paths, &log_path),
        ElevatedOperation::PipUpdate => run_pip_update(paths, &log_path, update_package),
        ElevatedOperation::ApplicationUpdate => run_application_update(paths, id, &log_path),
    };
    match result {
        Ok(()) => {
            write_operation_status(
                &status_path,
                OperationStatus {
                    operation: operation.name().into(),
                    state: "succeeded".into(),
                    message: "操作已完成。".into(),
                    success: Some(true),
                },
            )?;
            Ok(())
        }
        Err(error) => {
            let message = redact_secret_like_tokens(&format!("{error:#}"));
            let _ = write_operation_status(
                &status_path,
                OperationStatus {
                    operation: operation.name().into(),
                    state: "failed".into(),
                    message: message.clone(),
                    success: Some(false),
                },
            );
            Err(anyhow!(
                "{} failed; log: {}; {message}",
                operation.name(),
                log_path.display()
            ))
        }
    }
}

fn run_logged(command: &mut Command, log_path: &Path) -> Result<()> {
    hide_window(command);
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command.spawn()?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| anyhow!("operation stdout was not captured"))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| anyhow!("operation stderr was not captured"))?;
    let (sender, receiver) = mpsc::channel();
    let stdout_reader = spawn_operation_output_reader("stdout", stdout, sender.clone());
    let stderr_reader = spawn_operation_output_reader("stderr", stderr, sender);
    let mut log_file = fs::File::create(log_path)?;

    let status = loop {
        match receiver.recv_timeout(Duration::from_millis(100)) {
            Ok((source, line)) => write_operation_log_line(&mut log_file, &source, &line)?,
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => {}
        }
        if let Some(status) = child.try_wait()? {
            break status;
        }
    };
    let _ = stdout_reader.join();
    let _ = stderr_reader.join();
    for (source, line) in receiver.try_iter() {
        write_operation_log_line(&mut log_file, &source, &line)?;
    }
    log_file.flush()?;
    if !status.success() {
        bail!("operation exited with {status}");
    }
    Ok(())
}

fn run_wxdb_update(paths: &AppPaths, log_path: &Path) -> Result<()> {
    let script = materialize_embedded_script("wxdb-update", EMBEDDED_RUNTIME_INSTALL_SCRIPT)?;
    run_logged(
        Command::new("powershell.exe")
            .args(["-NoProfile", "-ExecutionPolicy", "Bypass", "-File"])
            .arg(&script)
            .arg("-RootPath")
            .arg(&paths.working_dir)
            .arg("-ConfigPath")
            .arg(&paths.config_path)
            .arg("-ForceWxdbUpdate"),
        log_path,
    )
}

fn materialize_embedded_script(name: &str, contents: &str) -> Result<PathBuf> {
    let directory = std::env::temp_dir().join("SummaryAgent4GroupChat-elevated");
    fs::create_dir_all(&directory)?;
    let path = directory.join(format!("{}-{name}.ps1", Uuid::new_v4()));
    fs::write(&path, contents)?;
    Ok(path)
}

fn run_pip_update(paths: &AppPaths, log_path: &Path, update_package: Option<&str>) -> Result<()> {
    let package = update_package
        .filter(|package| is_safe_pip_package_name(package))
        .ok_or_else(|| anyhow!("pip update requires a valid package name"))?;
    let config = AgentConfig::from_path(&paths.config_path)?;
    let python = configured_program(paths, &config.wx4py.python_executable);
    let mut command = Command::new(python);
    command
        .args([
            "-m",
            "pip",
            "install",
            "--disable-pip-version-check",
            "--upgrade",
            package,
        ])
        .current_dir(&paths.working_dir);
    run_logged(&mut command, log_path)
}

fn run_application_update(paths: &AppPaths, id: &str, log_path: &Path) -> Result<()> {
    let update_dir = paths.working_dir.join("runtime").join("updates");
    fs::create_dir_all(&update_dir)?;
    let script_path = update_dir.join(format!("{id}-application-update.ps1"));
    fs::write(
        &script_path,
        r#"param([Parameter(Mandatory = $true)][string]$Destination)
$ErrorActionPreference = "Stop"
$ProgressPreference = "SilentlyContinue"
$headers = @{ "User-Agent" = "SummaryAgent4GroupChat updater" }
$release = Invoke-RestMethod -Uri "https://api.github.com/repos/fangbm/SummaryAgent4GroupChat/releases/latest" -Headers $headers
$asset = @($release.assets | Where-Object { $_.name -like "SummaryAgent4GroupChat-Inno-Setup-windows-x64-*.exe" } | Select-Object -First 1)
if (-not $asset) { throw "最新 Release 未包含 Windows x64 Inno 安装包。" }
New-Item -ItemType Directory -Force -Path $Destination | Out-Null
$installer = Join-Path $Destination $asset.name
Write-Output "[update] 正在下载 $($asset.name)..."
Invoke-WebRequest -Uri $asset.browser_download_url -OutFile $installer -Headers $headers
$sumsAsset = @($release.assets | Where-Object { $_.name -eq "SHA256SUMS.txt" } | Select-Object -First 1)
if (-not $sumsAsset) { throw "最新 Release 缺少 SHA256SUMS.txt，拒绝安装未校验的更新。" }
$sumsPath = Join-Path $Destination "SHA256SUMS.txt"
Invoke-WebRequest -Uri $sumsAsset.browser_download_url -OutFile $sumsPath -Headers $headers
$expected = $null
foreach ($line in Get-Content -LiteralPath $sumsPath) {
    $parts = $line.Trim() -split '\s+', 2
    if ($parts.Count -eq 2 -and $parts[1] -eq $asset.name) { $expected = $parts[0].ToLowerInvariant(); break }
}
if (-not $expected) { throw "SHA256SUMS.txt 中没有 $($asset.name) 的校验值，拒绝安装。" }
$actual = (Get-FileHash -LiteralPath $installer -Algorithm SHA256).Hash.ToLowerInvariant()
if ($actual -ne $expected) {
    Remove-Item -LiteralPath $installer -Force
    throw "安装包 SHA256 校验失败（期望 $expected，实际 $actual），已删除下载文件。"
}
Write-Output "[update] SHA256 校验通过，正在启动安装程序。"
Start-Process -FilePath $installer
"#,
    )?;
    run_logged(
        Command::new("powershell.exe")
            .args(["-NoProfile", "-ExecutionPolicy", "Bypass", "-File"])
            .arg(script_path)
            .arg("-Destination")
            .arg(update_dir),
        log_path,
    )
}

fn is_safe_pip_package_name(package: &str) -> bool {
    !package.is_empty()
        && package.len() <= 128
        && package.chars().all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.')
        })
}

fn spawn_operation_output_reader<R>(
    source: &'static str,
    input: R,
    sender: mpsc::Sender<(String, String)>,
) -> thread::JoinHandle<()>
where
    R: Read + Send + 'static,
{
    thread::spawn(move || {
        let mut reader = BufReader::new(input);
        let mut bytes = Vec::new();
        loop {
            bytes.clear();
            match reader.read_until(b'\n', &mut bytes) {
                Ok(0) | Err(_) => break,
                Ok(_) => {
                    let line = String::from_utf8_lossy(&bytes)
                        .trim_end_matches(['\r', '\n'])
                        .to_owned();
                    if !line.is_empty() && sender.send((source.into(), line)).is_err() {
                        break;
                    }
                }
            }
        }
    })
}

fn write_operation_log_line(log_file: &mut fs::File, source: &str, line: &str) -> Result<()> {
    writeln!(log_file, "[{source}] {}", redact_secret_like_tokens(line))?;
    log_file.flush()?;
    Ok(())
}

fn operation_paths(paths: &AppPaths, id: &str) -> Result<(PathBuf, PathBuf)> {
    let safe_id: String = id
        .chars()
        .filter(|character| character.is_ascii_alphanumeric() || *character == '-')
        .collect();
    if safe_id.is_empty() {
        bail!("invalid operation id");
    }
    let directory = paths.working_dir.join("runtime").join("control-operations");
    fs::create_dir_all(&directory)?;
    Ok((
        directory.join(format!("{safe_id}.log")),
        directory.join(format!("{safe_id}.status.json")),
    ))
}

fn write_operation_status(path: &Path, status: OperationStatus) -> Result<()> {
    fs::write(path, serde_json::to_vec(&status)?)?;
    Ok(())
}

fn spawn_operation_monitor(
    state: ControlState,
    id: String,
    operation: String,
    log_path: PathBuf,
    status_path: PathBuf,
) {
    thread::spawn(move || {
        let started = Instant::now();
        let mut offset = 0;
        let mut previous_state = String::new();
        // The monitor writes "pending" before spawning the elevated process, so
        // an unknown state is treated as still waiting for elevation.
        let mut last_known_state = String::from("pending");
        loop {
            if let Ok(mut file) = fs::OpenOptions::new().read(true).open(&log_path) {
                if file.seek(SeekFrom::Start(offset)).is_ok() {
                    let mut bytes = Vec::new();
                    if file.read_to_end(&mut bytes).is_ok() && !bytes.is_empty() {
                        offset += bytes.len() as u64;
                        for line in String::from_utf8_lossy(&bytes).lines() {
                            if line.is_empty() {
                                continue;
                            }
                            let (source, message) = line
                                .strip_prefix("[stdout] ")
                                .map(|value| ("stdout", value))
                                .or_else(|| {
                                    line.strip_prefix("[stderr] ")
                                        .map(|value| ("stderr", value))
                                })
                                .unwrap_or(("output", line));
                            emit(
                                &state,
                                "operation.progress",
                                json!({ "id": id, "operation": operation, "source": source, "message": message }),
                            );
                        }
                    }
                }
            }

            if let Ok(text) = fs::read_to_string(&status_path) {
                if let Ok(status) = serde_json::from_str::<OperationStatus>(&text) {
                    last_known_state = status.state.clone();
                    if status.state != previous_state {
                        previous_state = status.state.clone();
                        emit(
                            &state,
                            "operation.progress",
                            json!({ "id": id, "operation": operation, "state": status.state, "message": status.message }),
                        );
                    }
                    if matches!(status.state.as_str(), "succeeded" | "failed") {
                        emit(
                            &state,
                            "operation.completed",
                            json!({ "id": id, "operation": operation, "success": status.success.unwrap_or(false), "message": status.message }),
                        );
                        return;
                    }
                }
            }

            // Only give up while still waiting for the UAC prompt; a running
            // elevated operation may legitimately take longer than the wait.
            let waiting_for_elevation = matches!(last_known_state.as_str(), "pending" | "");
            if waiting_for_elevation && started.elapsed() > ELEVATION_CONFIRM_TIMEOUT {
                emit(
                    &state,
                    "operation.completed",
                    json!({ "id": id, "operation": operation, "success": false, "message": "等待管理员权限确认超过 5 分钟，操作未启动。" }),
                );
                return;
            }
            thread::sleep(Duration::from_millis(200));
        }
    });
}

fn path_open(state: &ControlState, params: &Value) -> Result<Value> {
    let kind = params
        .get("kind")
        .and_then(Value::as_str)
        .unwrap_or("output");
    let path = match kind {
        "config" => state.paths.config_path.clone(),
        "output" => runtime_output_dir(&state.paths)?,
        "logs" => runtime_output_dir(&state.paths)?.join("wechat-summary-app.log"),
        _ => bail!("unsupported path kind"),
    };
    if kind == "output" {
        fs::create_dir_all(&path)?;
    }
    #[cfg(windows)]
    {
        Command::new("explorer.exe").arg(&path).spawn()?;
    }
    Ok(json!({ "opened": path }))
}

fn logs_tail(state: &ControlState) -> Result<Value> {
    let path = runtime_output_dir(&state.paths)?.join("wechat-summary-app.log");
    let text = match fs::read(&path) {
        Ok(bytes) => {
            let start = bytes.len().saturating_sub(64 * 1024);
            redact_secret_like_tokens(&String::from_utf8_lossy(&bytes[start..]))
        }
        Err(error) => format!("暂无日志：{error}"),
    };
    Ok(json!({ "path": path, "text": text }))
}

async fn runtime_check(state: &ControlState) -> Result<Value> {
    let config = AgentConfig::from_path(&state.paths.config_path)
        .context("reading configuration for runtime check")?;
    let platform = config.platform.kind.as_str();
    if platform != "wx" {
        return Ok(json!({
            "platform": platform,
            "ready": true,
            "missing": [],
            "detail": "当前平台不需要微信运行环境",
            "install_available": false,
        }));
    }

    let mut missing = Vec::new();
    let python = configured_program(&state.paths, &config.wx4py.python_executable);
    if run_hidden_command(
        &python,
        &[
            "-c",
            "import wx4py; print(getattr(wx4py, '__version__', 'installed'))",
        ],
        &state.paths.working_dir,
    )
    .await
    .is_err()
    {
        missing.push("Python 与 wx4py".to_string());
    }

    let wxdb_configured = config.wx_cli.executable.trim();
    if !wxdb_configured.eq_ignore_ascii_case("builtin") {
        let wxdb = configured_program(&state.paths, wxdb_configured);
        if run_hidden_command(&wxdb, &["--version"], &state.paths.working_dir)
            .await
            .is_err()
        {
            missing.push("wxdb".to_string());
        }
    }

    let ready = missing.is_empty();
    let detail = if ready {
        "微信运行环境已就绪；wxdb init 可在微信登录后按需运行。".to_string()
    } else {
        format!(
            "缺少：{}。可使用安装微信运行环境完成安装。",
            missing.join("、")
        )
    };
    Ok(json!({
        "platform": platform,
        "ready": ready,
        "missing": missing,
        "detail": detail,
        "install_available": !ready,
    }))
}

const UPDATE_CHECK_TIMEOUT: Duration = Duration::from_secs(20);
const APPLICATION_RELEASE_REPOSITORY: &str = "fangbm/SummaryAgent4GroupChat";
const WXDB_RELEASE_REPOSITORY: &str = "fangbm/wxdb";

async fn update_check(state: &ControlState) -> Result<Value> {
    let config = AgentConfig::from_path(&state.paths.config_path)
        .context("reading configuration for update check")?;
    let mut client_builder = reqwest::Client::builder()
        .timeout(UPDATE_CHECK_TIMEOUT)
        .user_agent("SummaryAgent4GroupChat/update-check");
    if config.proxy.enabled {
        if let Some(proxy_url) = config.proxy.https.as_ref().or(config.proxy.http.as_ref()) {
            client_builder = client_builder
                .proxy(reqwest::Proxy::all(proxy_url).context("configuring update check proxy")?);
        }
    }
    let client = client_builder
        .build()
        .context("creating update check HTTP client")?;

    let application = check_github_release(
        &client,
        "SummaryAgent4GroupChat",
        APPLICATION_RELEASE_REPOSITORY,
        Some(env!("CARGO_PKG_VERSION")),
    );
    let wxdb = check_wxdb_release(&client, &state.paths, &config.wx_cli.executable);
    let python = check_python_dependencies(&state.paths, &config.wx4py.python_executable);
    let (application, wxdb, mut python_dependencies) = tokio::join!(application, wxdb, python);

    let mut entries = vec![application, wxdb];
    entries.append(&mut python_dependencies);
    let update_count = entries
        .iter()
        .filter(|entry| {
            entry
                .get("update_available")
                .and_then(Value::as_bool)
                .unwrap_or(false)
        })
        .count();
    Ok(json!({
        "entries": entries,
        "update_count": update_count,
        "managed_scope": "应用本体、独立 wxdb 和当前 Python 虚拟环境中的 pip 依赖",
    }))
}

async fn check_github_release(
    client: &reqwest::Client,
    name: &str,
    repository: &str,
    current_version: Option<&str>,
) -> Value {
    let endpoint = format!("https://api.github.com/repos/{repository}/releases/latest");
    let response = match client.get(&endpoint).send().await {
        Ok(response) => response,
        Err(error) => {
            return update_entry(
                name,
                current_version,
                None,
                "unavailable",
                format!("无法检查 Release：{error}"),
            )
        }
    };
    let status = response.status();
    if !status.is_success() {
        return update_entry(
            name,
            current_version,
            None,
            "unavailable",
            format!("Release 服务返回 HTTP {status}"),
        );
    }
    let release = match response.json::<Value>().await {
        Ok(release) => release,
        Err(error) => {
            return update_entry(
                name,
                current_version,
                None,
                "unavailable",
                format!("Release 响应格式无效：{error}"),
            )
        }
    };
    let latest = release
        .get("tag_name")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let detail = release
        .get("html_url")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| "已获取最新 Release 信息".to_string());
    let Some(latest) = latest else {
        return update_entry(
            name,
            current_version,
            None,
            "unavailable",
            "Release 未包含版本标签",
        );
    };

    let status = match current_version {
        Some(current) if version_is_newer(latest, current) => "update_available",
        Some(_) => "up_to_date",
        None => "available_unknown_current",
    };
    update_entry(name, current_version, Some(latest), status, detail)
}

async fn check_wxdb_release(client: &reqwest::Client, paths: &AppPaths, configured: &str) -> Value {
    if configured.trim().eq_ignore_ascii_case("builtin") {
        return update_entry(
            "wxdb",
            Some("内置读取器"),
            None,
            "not_managed",
            "当前使用内置读取器，不依赖外部 wxdb Release",
        );
    }

    let executable = configured_program(paths, configured);
    let current = match run_hidden_command(&executable, &["--version"], &paths.working_dir).await {
        Ok(output) => version_from_text(&output).or_else(|| non_empty_line(&output)),
        Err(_) => None,
    };
    let mut entry =
        check_github_release(client, "wxdb", WXDB_RELEASE_REPOSITORY, current.as_deref()).await;
    if current.is_none() {
        entry["status"] = Value::String("not_detected".into());
        entry["update_available"] = Value::Bool(false);
        entry["detail"] = Value::String(format!(
            "未能执行 {} --version；请检查 wxdb 路径或 PATH",
            executable.display()
        ));
    }
    entry
}

async fn check_python_dependencies(paths: &AppPaths, configured: &str) -> Vec<Value> {
    let python = configured_program(paths, configured);
    let version = run_hidden_command(&python, &["--version"], &paths.working_dir).await;
    let current_version = match version {
        Ok(output) => version_from_text(&output).or_else(|| non_empty_line(&output)),
        Err(error) => {
            return vec![update_entry(
                "Python 虚拟环境",
                None,
                None,
                "not_detected",
                format!("未能执行 {}：{error:#}", python.display()),
            )]
        }
    };
    let mut entries = vec![update_entry(
        "Python 虚拟环境",
        current_version.as_deref(),
        None,
        "installed",
        "已检查解释器；下方列出当前虚拟环境中可升级的 pip 包",
    )];
    let output = match run_hidden_command(
        &python,
        &["-m", "pip", "list", "--outdated", "--format=json"],
        &paths.working_dir,
    )
    .await
    {
        Ok(output) => output,
        Err(error) => {
            entries.push(update_entry(
                "Python pip 依赖",
                None,
                None,
                "unavailable",
                format!("无法检查 pip 更新：{error:#}"),
            ));
            return entries;
        }
    };
    let packages = match serde_json::from_str::<Value>(&output)
        .ok()
        .and_then(|value| value.as_array().cloned())
    {
        Some(packages) => packages,
        None => {
            entries.push(update_entry(
                "Python pip 依赖",
                None,
                None,
                "unavailable",
                "pip 未返回预期 JSON，无法判断依赖更新",
            ));
            return entries;
        }
    };
    if packages.is_empty() {
        entries.push(update_entry(
            "Python pip 依赖",
            Some("已检查"),
            Some("均为最新"),
            "up_to_date",
            "当前虚拟环境中没有可升级的 pip 包",
        ));
        return entries;
    }
    for package in packages {
        let name = package
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or("未知包");
        let current = package.get("version").and_then(Value::as_str);
        let latest = package.get("latest_version").and_then(Value::as_str);
        entries.push(update_entry(
            &format!("Python: {name}"),
            current,
            latest,
            "update_available",
            "虚拟环境 pip 依赖",
        ));
    }
    entries
}

async fn run_hidden_command(program: &Path, args: &[&str], working_dir: &Path) -> Result<String> {
    let mut command = AsyncCommand::new(program);
    command
        .args(args)
        .current_dir(working_dir)
        .stdin(Stdio::null())
        .kill_on_drop(true);
    #[cfg(windows)]
    command.as_std_mut().creation_flags(CREATE_NO_WINDOW);
    let output = timeout(UPDATE_CHECK_TIMEOUT, command.output())
        .await
        .map_err(|_| anyhow!("命令在 {} 秒内未完成", UPDATE_CHECK_TIMEOUT.as_secs()))??;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        bail!("命令退出码 {}：{}", output.status, stderr);
    }
    Ok(format!(
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    ))
}

fn configured_program(paths: &AppPaths, configured: &str) -> PathBuf {
    let path = PathBuf::from(configured.trim());
    if path.is_absolute() || !configured.contains(['\\', '/']) {
        path
    } else {
        paths.working_dir.join(path)
    }
}

fn update_entry(
    name: &str,
    current_version: Option<&str>,
    latest_version: Option<&str>,
    status: &str,
    detail: impl Into<String>,
) -> Value {
    json!({
        "name": name,
        "current_version": current_version,
        "latest_version": latest_version,
        "status": status,
        "update_available": status == "update_available",
        "detail": detail.into(),
    })
}

fn version_from_text(text: &str) -> Option<String> {
    text.split(|character: char| character.is_whitespace() || character == ',')
        .find_map(|token| {
            version_components(token).map(|components| {
                components
                    .into_iter()
                    .map(|part| part.to_string())
                    .collect::<Vec<_>>()
                    .join(".")
            })
        })
}

fn non_empty_line(text: &str) -> Option<String> {
    text.lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .map(ToOwned::to_owned)
}

fn version_is_newer(candidate: &str, current: &str) -> bool {
    let Some(candidate) = version_components(candidate) else {
        return false;
    };
    let Some(current) = version_components(current) else {
        return false;
    };
    let width = candidate.len().max(current.len());
    for index in 0..width {
        match candidate
            .get(index)
            .copied()
            .unwrap_or(0)
            .cmp(&current.get(index).copied().unwrap_or(0))
        {
            std::cmp::Ordering::Greater => return true,
            std::cmp::Ordering::Less => return false,
            std::cmp::Ordering::Equal => {}
        }
    }
    false
}

fn version_components(value: &str) -> Option<Vec<u64>> {
    let value = value.trim().trim_start_matches(['v', 'V']);
    let version = value
        .chars()
        .take_while(|character| character.is_ascii_digit() || *character == '.')
        .collect::<String>();
    if version.is_empty()
        || !version
            .chars()
            .next()
            .is_some_and(|character| character.is_ascii_digit())
    {
        return None;
    }
    version
        .split('.')
        .map(|part| {
            (!part.is_empty())
                .then(|| part.parse::<u64>().ok())
                .flatten()
        })
        .collect()
}

fn runtime_output_dir(paths: &AppPaths) -> Result<PathBuf> {
    let raw = fs::read_to_string(&paths.config_path)?;
    let value: toml::Value = raw.parse()?;
    let configured = value
        .get("runtime")
        .and_then(|runtime| runtime.get("output_dir"))
        .and_then(toml::Value::as_str)
        .unwrap_or("runtime/rust-output");
    let path = PathBuf::from(configured);
    Ok(if path.is_absolute() {
        path
    } else {
        paths.working_dir.join(path)
    })
}

fn find_program(paths: &AppPaths, stem: &str) -> Result<PathBuf> {
    let name = format!("{stem}.exe");
    let current = env::current_exe()?;
    let candidates = [
        paths.working_dir.join("bin").join(&name),
        current
            .parent()
            .map(|dir| dir.join(&name))
            .unwrap_or_default(),
        current
            .parent()
            .and_then(Path::parent)
            .map(|root| root.join("bin").join(&name))
            .unwrap_or_default(),
    ];
    candidates
        .into_iter()
        .find(|candidate| candidate.is_file())
        .ok_or_else(|| anyhow!("cannot find {name}"))
}

fn spawn_output_reader(
    events: broadcast::Sender<Event>,
    stream: impl std::io::Read + Send + 'static,
    source: &'static str,
) {
    thread::spawn(move || {
        for result in BufReader::new(stream).lines() {
            match result {
                Ok(line) => {
                    let _ = events.send(Event { version: PROTOCOL_VERSION, event: "output".into(), data: json!({ "source": source, "text": redact_secret_like_tokens(&line), "level": classify_level(&line) }) });
                }
                Err(error) => {
                    let _ = events.send(Event { version: PROTOCOL_VERSION, event: "output".into(), data: json!({ "source": source, "text": format!("读取进程输出失败：{error}"), "level": "error" }) });
                    break;
                }
            }
        }
    });
}

fn classify_level(text: &str) -> &'static str {
    let upper = text.to_ascii_uppercase();
    if upper.contains("ERROR") || upper.contains("FAILED") {
        "error"
    } else if upper.contains("WARN") {
        "warning"
    } else if upper.contains("INFO") {
        "info"
    } else {
        "normal"
    }
}

fn emit(state: &ControlState, event: &str, data: Value) {
    let _ = state.events.send(Event {
        version: PROTOCOL_VERSION,
        event: event.into(),
        data,
    });
}

fn hide_window(command: &mut Command) {
    #[cfg(windows)]
    {
        command.creation_flags(CREATE_NO_WINDOW);
    }
    #[cfg(not(windows))]
    {
        let _ = command;
    }
}

fn ps_quote(input: &str) -> String {
    input.replace('\'', "''")
}

/// Constant-time token comparison. The length check leaks only the length,
/// which is fixed for the GUID-hex tokens this service generates.
fn tokens_match(provided: &str, expected: &str) -> bool {
    let provided = provided.as_bytes();
    let expected = expected.as_bytes();
    if provided.len() != expected.len() {
        return false;
    }
    provided
        .iter()
        .zip(expected.iter())
        .fold(0u8, |accumulator, (left, right)| {
            accumulator | (left ^ right)
        })
        == 0
}

fn redact_secret_like_tokens(input: &str) -> String {
    let mut result = input.to_string();
    for marker in ["sk-", "Bearer "] {
        let mut offset = 0;
        while let Some(found) = result[offset..].find(marker) {
            let start = offset + found;
            let end = result[start + marker.len()..]
                .find(|character: char| {
                    character.is_whitespace() || matches!(character, '\"' | '\'' | ',' | ')')
                })
                .map(|relative| start + marker.len() + relative)
                .unwrap_or(result.len());
            if end.saturating_sub(start) > marker.len() + 8 {
                result.replace_range(start..end, "<redacted-secret>");
                offset = start + 18;
            } else {
                offset = end;
            }
        }
    }
    result
}

#[cfg(windows)]
impl Job {
    fn assign(child: &Child) -> Result<Self> {
        use windows_sys::Win32::{
            Foundation::{CloseHandle, INVALID_HANDLE_VALUE},
            System::JobObjects::{
                AssignProcessToJobObject, CreateJobObjectW, JobObjectExtendedLimitInformation,
                SetInformationJobObject, JOBOBJECT_EXTENDED_LIMIT_INFORMATION,
                JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
            },
        };
        let handle = unsafe { CreateJobObjectW(std::ptr::null(), std::ptr::null()) };
        if handle.is_null() || handle == INVALID_HANDLE_VALUE {
            return Err(std::io::Error::last_os_error().into());
        }
        let mut info: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = unsafe { std::mem::zeroed() };
        info.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
        let set = unsafe {
            SetInformationJobObject(
                handle,
                JobObjectExtendedLimitInformation,
                &info as *const _ as *const _,
                std::mem::size_of_val(&info) as u32,
            )
        };
        if set == 0 {
            unsafe {
                CloseHandle(handle);
            }
            return Err(std::io::Error::last_os_error().into());
        }
        let assigned = unsafe {
            AssignProcessToJobObject(
                handle,
                child.as_raw_handle() as windows_sys::Win32::Foundation::HANDLE,
            )
        };
        if assigned == 0 {
            unsafe {
                CloseHandle(handle);
            }
            return Err(std::io::Error::last_os_error().into());
        }
        Ok(Self(handle))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redacted_secret_is_preserved_on_write() {
        let mut submitted: toml::Value = "[llm]\napi_key = \"<redacted-secret>\"\nmodel = \"new\""
            .parse()
            .unwrap();
        let existing: toml::Value = "[llm]\napi_key = \"sk-actual\"\nmodel = \"old\""
            .parse()
            .unwrap();
        preserve_redacted_secrets(&mut submitted, &existing, None);
        assert_eq!(submitted["llm"]["api_key"].as_str(), Some("sk-actual"));
    }

    #[test]
    fn redact_toml_hides_keys_and_tokens() {
        let text = "[llm]\napi_key = \"sk-actual-secret\"\nmodel = \"model\"\n";
        let output = redact_toml_secrets(text).unwrap();
        assert!(!output.contains("actual-secret"));
        assert!(output.contains("<redacted-secret>"));
    }

    #[test]
    fn version_comparison_handles_tags_and_component_width() {
        assert!(version_is_newer("v0.1.10", "0.1.4"));
        assert!(version_is_newer("1.2.0", "1.1.99"));
        assert!(!version_is_newer("v0.1.4", "0.1.4"));
        assert!(!version_is_newer("v0.1.3", "0.1.4"));
        assert!(!version_is_newer("not-a-version", "0.1.4"));
    }

    #[test]
    fn version_parser_extracts_tool_version_from_command_output() {
        assert_eq!(
            version_from_text("wxdb version v0.1.4\n").as_deref(),
            Some("0.1.4")
        );
        assert_eq!(
            version_from_text("Python 3.12.2").as_deref(),
            Some("3.12.2")
        );
    }

    #[test]
    fn pip_update_package_names_are_strictly_validated() {
        assert!(is_safe_pip_package_name("beautifulsoup4"));
        assert!(is_safe_pip_package_name("my-package_2.0"));
        assert!(!is_safe_pip_package_name("package; whoami"));
        assert!(!is_safe_pip_package_name("../package"));
    }

    #[test]
    fn token_comparison_is_exact_and_length_safe() {
        let token = "a1b2c3d4e5f6a7b8a1b2c3d4e5f6a7b8";
        assert!(tokens_match(token, token));
        assert!(tokens_match("", ""));
        assert!(!tokens_match("short", "shorter"));
        assert!(!tokens_match("a1b2c3d4e5f6a7b8a1b2c3d4e5f6a7b9", token));
        assert!(!tokens_match("", token));
    }

    #[test]
    fn config_patch_sets_updates_and_removes_keys() {
        let raw = "# header comment\n[llm]\nmodel = \"old\" # keep\nstream = true\n\n[listen]\ntriggers = [\"/总结\"]\n";

        let params: ConfigPatchParams = serde_json::from_value(json!({
            "operations": [
                { "section": ["llm"], "key": "model", "value": "gpt-new" },
                { "section": ["llm"], "key": "timeout_seconds", "value": 90 },
                { "section": ["listen"], "key": "ignore_self", "value": false },
                { "section": ["llm"], "key": "stream", "value": null }
            ]
        }))
        .unwrap();
        let patched = apply_config_patch(raw, &params).unwrap();

        assert!(patched.contains("model = \"gpt-new\""));
        assert!(patched.contains("timeout_seconds = 90"));
        assert!(patched.contains("ignore_self = false"));
        assert!(!patched.contains("stream"));
        // Format preservation: comments and untouched keys survive.
        assert!(patched.contains("# header comment"));
        assert!(patched.contains("# keep"));
        assert!(patched.contains("triggers = [\"/总结\"]"));
    }

    #[test]
    fn config_patch_supports_inline_tables_and_room_capabilities() {
        let raw = "[wx4py]\ngroups = [\"群A\"]\n\n[room_capabilities]\n\"旧群\" = { image_summary_enabled = false }\n";

        let params: ConfigPatchParams = serde_json::from_value(json!({
            "operations": [
                { "section": ["room_capabilities"], "key": "旧群", "value": null },
                { "section": ["room_capabilities"], "key": "新群", "value": { "image_summary_enabled": false } }
            ]
        }))
        .unwrap();
        let patched = apply_config_patch(raw, &params).unwrap();

        assert!(!patched.contains("旧群"));
        assert!(patched.contains("image_summary_enabled = false"));
        let value: toml::Value = patched.parse().unwrap();
        assert_eq!(
            value["room_capabilities"]["新群"]["image_summary_enabled"].as_bool(),
            Some(false)
        );
    }

    #[test]
    fn config_patch_rejects_bad_operations() {
        let raw = "[llm]\nmodel = \"x\"\n";
        let params: ConfigPatchParams = serde_json::from_value(json!({
            "operations": [ { "section": [], "key": "model", "value": "y" } ]
        }))
        .unwrap();
        assert!(apply_config_patch(raw, &params).is_err());

        let params: ConfigPatchParams = serde_json::from_value(json!({
            "operations": [ { "section": ["llm"], "key": "", "value": "y" } ]
        }))
        .unwrap();
        assert!(apply_config_patch(raw, &params).is_err());
    }

    #[test]
    fn env_var_name_keys_are_not_redacted() {
        assert!(!is_secret_key("api_key_env"));
        assert!(!is_secret_key("token_env"));
        assert!(is_secret_key("api_key"));
        assert!(is_secret_key("api_keys"));
        assert!(is_secret_key("token"));
        assert!(is_secret_key("download_secret"));
    }

    #[cfg(windows)]
    #[test]
    fn logged_operation_keeps_stdout_and_stderr() {
        let log_path =
            std::env::temp_dir().join(format!("control-operation-{}.log", Uuid::new_v4()));
        let mut command = Command::new("cmd.exe");
        command.args(["/C", "echo standard-output & echo standard-error 1>&2"]);
        run_logged(&mut command, &log_path).unwrap();
        let text = fs::read_to_string(&log_path).unwrap();
        let _ = fs::remove_file(&log_path);
        assert!(text.contains("[stdout] standard-output"));
        assert!(text.contains("[stderr] standard-error"));
    }
}
