#![cfg_attr(windows, windows_subsystem = "windows")]

//! Local control plane for the native Windows UI.
//! The GUI never owns configuration parsing or agent processes directly.

use std::{
    env, fs,
    io::{BufRead, BufReader},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{Arc, Mutex},
    thread,
};

#[cfg(windows)]
use std::os::windows::{io::AsRawHandle, process::CommandExt};

use anyhow::{anyhow, bail, Context, Result};
use clap::{Parser, ValueEnum};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader as AsyncBufReader},
    sync::broadcast,
};
use uuid::Uuid;
use wechat_summary_core::AgentConfig;

const PROTOCOL_VERSION: u32 = 1;
const CREATE_NO_WINDOW: u32 = 0x0800_0000;
const MAX_REQUEST_BYTES: usize = 2 * 1024 * 1024;

#[derive(Debug, Parser)]
#[command(name = "wechat-summary-control")]
#[command(about = "Local control service for SummaryAgent4GroupChat")]
struct Args {
    #[arg(long, default_value = r"\\.\pipe\SummaryAgent4GroupChat.Control.v1")]
    pipe: String,
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
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum ElevatedOperation {
    RuntimeInstall,
    WxdbInit,
}

impl ElevatedOperation {
    fn name(self) -> &'static str {
        match self {
            Self::RuntimeInstall => "runtime.install",
            Self::WxdbInit => "wxdb.init",
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
        return run_elevated(operation, &paths, args.operation_id.as_deref());
    }

    let token = args
        .token
        .filter(|token| !token.trim().is_empty())
        .ok_or_else(|| anyhow!("--token is required for control service"))?;
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
    if request.version != PROTOCOL_VERSION || request.token != token {
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
        "output.subscribe" | "logs.subscribe" | "status.subscribe"
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
                if topic == "output" && event.event == "output"
                    || topic == "logs" && event.event == "log"
                    || topic == "status" && event.event == "status.changed" =>
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
        "agent.start" => agent_start(state),
        "agent.stop" => agent_stop(state),
        "runtime.install" => start_elevated_operation(state, ElevatedOperation::RuntimeInstall),
        "wxdb.init" => start_elevated_operation(state, ElevatedOperation::WxdbInit),
        "path.open" => path_open(state, params),
        "logs.tail" => logs_tail(state),
        "output.subscribe" | "logs.subscribe" | "status.subscribe" => {
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
    Ok(
        json!({ "toml": redact_toml_secrets(&raw)?, "validation": validation, "path": state.paths.config_path }),
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
    key == "token" || key.contains("api_key") || key.contains("secret") || key.contains("password")
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
    let id = Uuid::new_v4().to_string();
    let executable = env::current_exe()?;
    let arguments = format!(
        "--elevated {} --config \"{}\" --working-dir \"{}\" --operation-id {}",
        operation.to_possible_value().expect("value").get_name(),
        state.paths.config_path.display(),
        state.paths.working_dir.display(),
        id
    );
    let mut command = Command::new("powershell.exe");
    command.args([
        "-NoProfile",
        "-NonInteractive",
        "-Command",
        &format!(
            "Start-Process -FilePath '{}' -ArgumentList '{}' -Verb RunAs",
            ps_quote(&executable.display().to_string()),
            ps_quote(&arguments)
        ),
    ]);
    hide_window(&mut command);
    command
        .spawn()
        .context("requesting elevation for approved operation")?;
    emit(
        state,
        "operation.progress",
        json!({ "id": id, "operation": operation.name(), "message": "已请求管理员权限" }),
    );
    Ok(json!({ "operation_id": id, "operation": operation.name(), "elevation_requested": true }))
}

fn run_elevated(
    operation: ElevatedOperation,
    paths: &AppPaths,
    operation_id: Option<&str>,
) -> Result<()> {
    let id = operation_id.unwrap_or("manual");
    let log_dir = paths.working_dir.join("runtime").join("control-operations");
    fs::create_dir_all(&log_dir)?;
    let log_path = log_dir.join(format!("{id}.log"));
    let result = match operation {
        ElevatedOperation::RuntimeInstall => {
            let script = if paths.working_dir.join("install.ps1").exists() {
                paths.working_dir.join("install.ps1")
            } else {
                paths
                    .working_dir
                    .parent()
                    .unwrap_or(&paths.working_dir)
                    .join("scripts")
                    .join("install-python-runtime.ps1")
            };
            run_logged(
                Command::new("powershell.exe")
                    .args(["-NoProfile", "-ExecutionPolicy", "Bypass", "-File"])
                    .arg(script)
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
    };
    result.map_err(|error| {
        anyhow!(
            "{} failed; log: {}; {error:#}",
            operation.name(),
            log_path.display()
        )
    })
}

fn run_logged(command: &mut Command, log_path: &Path) -> Result<()> {
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let output = command.output()?;
    let text = format!(
        "[stdout]\n{}\n[stderr]\n{}\n",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    fs::write(log_path, redact_secret_like_tokens(&text))?;
    if !output.status.success() {
        bail!("operation exited with {}", output.status);
    }
    Ok(())
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
}
