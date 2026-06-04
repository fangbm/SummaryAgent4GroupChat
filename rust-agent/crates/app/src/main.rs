use std::{
    collections::{HashMap, VecDeque},
    env,
    fs::{self, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration as StdDuration, SystemTime},
};

mod platform;

use anyhow::{bail, Context, Result};
use chrono::{DateTime, Duration, TimeZone, Utc};
use tokio::task::JoinSet;
use tracing::{error, info, warn};
use tracing_subscriber::{fmt::MakeWriter, EnvFilter};
use wechat_summary_ai::{
    AiError, AiRetryNotice, OpenAiCompatibleLlm, OpenAiImageClient, OpenAiVisionCaptionClient,
    RetryNotifier,
};
use wechat_summary_core::{
    config::{PlatformKindConfig, TimeRangeMode},
    models::{ChatMessage, ImageArtifact, IncomingMessage},
    parse_command_time_range_minutes, AgentConfig, ChatFormatter, PrivacyFilter, ResolvedTimeRange,
    TimeRangeCalculator, TriggerMatch, TriggerMatcher,
};
use wechat_summary_storage::SqliteStateStore;

use crate::platform::{PlatformClient, PlatformEvent, PlatformHistoryMessage, PlatformSender};

const TRIGGER_DEDUPE_WINDOW_SECONDS: i64 = 15;
const RECENT_OBSERVED_WINDOW_HOURS: i64 = 6;
const RECENT_OBSERVED_MAX_MESSAGES: usize = 5_000;
const EMPTY_HISTORY_RETRY_DELAYS_MS: &[u64] = &[1_500, 3_000, 5_000];
const LLM_RATE_LIMIT_QUEUE_DELAY_SECONDS: u64 = 60;
const LLM_RATE_LIMIT_QUEUE_MAX_ATTEMPTS: usize = 3;
const CHUNK_PROMPT_HEADROOM_CHARS: usize = 4_096;

#[tokio::main]
async fn main() -> Result<()> {
    let config_path = config_path_from_args();
    if let Err(error) = run_agent(&config_path).await {
        append_startup_error(&config_path, &format!("fatal startup error: {error:#}"));
        return Err(error);
    }
    Ok(())
}

async fn run_agent(config_path: &str) -> Result<()> {
    let mut config_reloader = ConfigReloader::load(config_path)?;
    let config = config_reloader.config();

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new(&config.runtime.log_level))
        .with_writer(RuntimeTraceWriter::new(config))
        .with_ansi(false)
        .init();

    append_runtime_log(config, "agent startup started");
    refresh_wxdb_keys_on_start(config);

    let store = SqliteStateStore::open(&config.storage.sqlite_path)
        .with_context(|| format!("opening state store {}", config.storage.sqlite_path))?;
    let client = PlatformClient::start(config)
        .await
        .with_context(|| format!("starting {} platform client", config.platform.kind.as_str()))?;
    let platform_rooms = client.configured_rooms(config);
    let mut recent_trigger_attempts = RecentTriggerAttempts::default();
    let mut recent_observed_messages = RecentObservedMessages::default();

    info!(
        platform = client.kind().as_str(),
        rooms = ?platform_rooms,
        "platform message receiving enabled"
    );
    append_runtime_log(
        config,
        &format!(
            "platform enabled kind={} rooms={:?}",
            client.kind().as_str(),
            platform_rooms
        ),
    );

    let mut next_scheduled_run = next_scheduled_run_after(Utc::now(), config);
    if let Some(run_at) = next_scheduled_run {
        info!(
            run_at_utc = %run_at,
            run_at_beijing = %format_beijing_time(run_at),
            "scheduled summary enabled"
        );
        append_runtime_log(
            config,
            &format!(
                "scheduled summary enabled next_run_utc={} next_run_beijing={}",
                run_at,
                format_beijing_time(run_at)
            ),
        );
    }

    loop {
        reload_config_if_changed(&mut config_reloader, &mut next_scheduled_run)?;

        let now = Utc::now();
        let config = config_reloader.config();
        if next_scheduled_run.is_some_and(|run_at| now >= run_at) {
            run_scheduled_summaries(config, &store, &client, now).await?;
            let config = config_reloader.config();
            next_scheduled_run = next_scheduled_run_after(now + Duration::seconds(1), config);
            if let Some(run_at) = next_scheduled_run {
                info!(
                    run_at_utc = %run_at,
                    run_at_beijing = %format_beijing_time(run_at),
                    "next scheduled summary planned"
                );
            }
        }

        if let Some(event) = client.next_event_timeout(StdDuration::from_secs(1))? {
            reload_config_if_changed(&mut config_reloader, &mut next_scheduled_run)?;
            let config = config_reloader.config();
            let matcher = config_reloader.matcher();
            handle_platform_event(
                config,
                &store,
                matcher,
                &client,
                &mut recent_trigger_attempts,
                &mut recent_observed_messages,
                event,
            )
            .await?;
        }
    }
}

fn reload_config_if_changed(
    config_reloader: &mut ConfigReloader,
    next_scheduled_run: &mut Option<DateTime<Utc>>,
) -> Result<()> {
    if !config_reloader.reload_if_changed()? {
        return Ok(());
    }

    let config = config_reloader.config();
    *next_scheduled_run = next_scheduled_run_after(Utc::now(), config);
    if let Some(run_at) = next_scheduled_run {
        info!(
            run_at_utc = %run_at,
            run_at_beijing = %format_beijing_time(*run_at),
            "scheduled summary replanned after config reload"
        );
        append_runtime_log(
            config,
            &format!(
                "scheduled summary replanned after config reload next_run_utc={} next_run_beijing={}",
                run_at,
                format_beijing_time(*run_at)
            ),
        );
    } else {
        info!("scheduled summary disabled after config reload");
        append_runtime_log(config, "scheduled summary disabled after config reload");
    }

    Ok(())
}

struct ConfigReloader {
    path: PathBuf,
    config: AgentConfig,
    matcher: TriggerMatcher,
    last_modified: Option<SystemTime>,
    last_failed_modified: Option<SystemTime>,
}

impl ConfigReloader {
    fn load(config_path: &str) -> Result<Self> {
        let path = PathBuf::from(config_path);
        let config = AgentConfig::from_path(&path)
            .with_context(|| format!("loading config {}", path.display()))?;
        let matcher =
            TriggerMatcher::new(config.listen.clone()).context("building trigger matcher")?;
        let last_modified = config_modified_time(&path);
        Ok(Self {
            path,
            config,
            matcher,
            last_modified,
            last_failed_modified: None,
        })
    }

    fn config(&self) -> &AgentConfig {
        &self.config
    }

    fn matcher(&self) -> &TriggerMatcher {
        &self.matcher
    }

    fn reload_if_changed(&mut self) -> Result<bool> {
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
        let matcher = match TriggerMatcher::new(config.listen.clone()) {
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
                "config hot reloaded path={} note=startup-only settings still require restart: platform listener, storage path, runtime log writer",
                self.path.display()
            ),
        );
        Ok(true)
    }
}

fn config_modified_time(path: &Path) -> Option<SystemTime> {
    fs::metadata(path)
        .and_then(|metadata| metadata.modified())
        .ok()
}

fn refresh_wxdb_keys_on_start(config: &AgentConfig) {
    match wx4py_client::refresh_builtin_wxdb_keys_on_start(&config.wx_cli) {
        Ok(Some(reports)) => {
            let total_before: usize = reports.iter().map(|report| report.before_keys).sum();
            let total_scanned: usize = reports.iter().map(|report| report.scanned_keys).sum();
            let total_after: usize = reports.iter().map(|report| report.after_keys).sum();
            info!(
                stores = reports.len(),
                before_keys = total_before,
                scanned_keys = total_scanned,
                after_keys = total_after,
                "startup wxdb init completed"
            );
            append_runtime_log(
                config,
                &format!(
                    "startup wxdb init completed stores={} before_keys={} scanned_keys={} after_keys={}",
                    reports.len(),
                    total_before,
                    total_scanned,
                    total_after
                ),
            );
            for report in reports {
                if let Some(error) = report.scan_error {
                    warn!(
                        db_dir = %report.db_dir,
                        error = %error,
                        "startup wxdb init scan warning"
                    );
                    append_runtime_log(
                        config,
                        &format!(
                            "startup wxdb init scan warning db_dir={} error={}",
                            report.db_dir, error
                        ),
                    );
                } else {
                    append_runtime_log(
                        config,
                        &format!(
                            "startup wxdb init store db_dir={} before_keys={} imported_legacy_keys={} scanned_keys={} after_keys={}",
                            report.db_dir,
                            report.before_keys,
                            report.imported_legacy_keys,
                            report.scanned_keys,
                            report.after_keys
                        ),
                    );
                }
            }
        }
        Ok(None) => {
            info!("startup wxdb init skipped because external wxdb executable is configured");
            append_runtime_log(
                config,
                "startup wxdb init skipped external wxdb executable configured",
            );
        }
        Err(error) => {
            let error_message = format_error_chain(&anyhow::Error::new(error));
            warn!(error = %error_message, "startup wxdb init failed");
            append_runtime_log(
                config,
                &format!("startup wxdb init failed error={error_message}"),
            );
        }
    }
}

fn config_path_from_args() -> String {
    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--config" {
            if let Some(path) = args.next() {
                return path;
            }
        }
    }
    "config/agent.toml".to_string()
}

fn append_runtime_log(config: &AgentConfig, message: &str) {
    let output_dir = std::path::Path::new(&config.runtime.output_dir);
    if fs::create_dir_all(output_dir).is_err() {
        return;
    }
    let path = output_dir.join("wechat-summary-app.log");
    if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(path) {
        let _ = writeln!(file, "{} {}", Utc::now().to_rfc3339(), message);
    }
}

fn retry_message_notifier(
    config: &AgentConfig,
    sender: PlatformSender,
    room_id: String,
) -> RetryNotifier {
    let config = config.clone();
    Arc::new(move |notice: AiRetryNotice| {
        let config = config.clone();
        let sender = sender.clone();
        let room_id = room_id.clone();
        Box::pin(async move {
            let message = format_retry_notice(&notice);
            let reason = retry_notice_reason(&notice.reason, 160);
            info!(
                room_id = %room_id,
                operation = notice.operation,
                attempt = notice.attempt,
                max_attempts = notice.max_attempts,
                retry_after_ms = notice.retry_after_ms,
                reason = %reason,
                "sending AI retry notification"
            );
            append_runtime_log(
                &config,
                &format!(
                    "ai retry notification room={} operation={} retry={}/{} wait_ms={} reason={}",
                    room_id,
                    notice.operation,
                    retry_notice_retry_index(&notice),
                    retry_notice_max_retries(&notice),
                    notice.retry_after_ms,
                    reason
                ),
            );
            if let Err(error) = sender.send_text(&room_id, &message).await {
                let send_error = format_error_chain(&error);
                warn!(
                    room_id = %room_id,
                    error = %send_error,
                    "failed to send AI retry notification"
                );
                append_runtime_log(
                    &config,
                    &format!(
                        "ai retry notification send failed room={} error={}",
                        room_id, send_error
                    ),
                );
            }
        })
    })
}

fn format_retry_notice(notice: &AiRetryNotice) -> String {
    let operation = retry_operation_label(notice.operation);
    let retry_index = retry_notice_retry_index(notice);
    let max_retries = retry_notice_max_retries(notice);
    let wait = format_retry_wait(notice.retry_after_ms);
    let reason = retry_notice_reason(&notice.reason, 96);
    format!(
        "{operation}暂时失败，正在重试（第 {retry_index}/{max_retries} 次，约 {wait} 后继续）。原因：{reason}"
    )
}

fn retry_operation_label(operation: &str) -> &'static str {
    match operation {
        "LLM chat completion" => "模型请求",
        "image caption request" => "图片转述请求",
        "image caption remote image download" => "图片下载",
        "image generation request" => "图片生成请求",
        "image task poll" => "图片任务查询",
        "image download" => "生成图片下载",
        _ => "模型服务请求",
    }
}

fn retry_notice_max_retries(notice: &AiRetryNotice) -> usize {
    notice.max_attempts.saturating_sub(1).max(1)
}

fn retry_notice_retry_index(notice: &AiRetryNotice) -> usize {
    notice.attempt.min(retry_notice_max_retries(notice)).max(1)
}

fn format_retry_wait(ms: u64) -> String {
    if ms < 1_000 {
        return format!("{ms}毫秒");
    }
    if ms % 1_000 == 0 {
        return format!("{}秒", ms / 1_000);
    }
    format!("{:.1}秒", ms as f64 / 1_000.0)
}

fn retry_notice_reason(reason: &str, max_chars: usize) -> String {
    let compact = reason.split_whitespace().collect::<Vec<_>>().join(" ");
    let redacted = redact_secret_like_tokens(&compact);
    let mut output = redacted.chars().take(max_chars).collect::<String>();
    if redacted.chars().count() > max_chars {
        output.push_str("...");
    }
    if output.is_empty() {
        "unknown".to_string()
    } else {
        output
    }
}

fn redact_secret_like_tokens(input: &str) -> String {
    let mut output = String::with_capacity(input.len());
    let mut index = 0;
    while let Some(relative) = input[index..].find("sk-") {
        let start = index + relative;
        output.push_str(&input[index..start]);
        let mut end = start + 3;
        for (offset, ch) in input[end..].char_indices() {
            if ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' {
                end = start + 3 + offset + ch.len_utf8();
            } else {
                break;
            }
        }
        if end - start >= 12 {
            output.push_str("<redacted-secret>");
        } else {
            output.push_str(&input[start..end]);
        }
        index = end;
    }
    output.push_str(&input[index..]);
    output
}

#[derive(Clone)]
struct RuntimeTraceWriter {
    path: PathBuf,
}

impl RuntimeTraceWriter {
    fn new(config: &AgentConfig) -> Self {
        let output_dir = std::path::Path::new(&config.runtime.output_dir);
        let _ = fs::create_dir_all(output_dir);
        Self {
            path: output_dir.join("wechat-summary-app.log"),
        }
    }
}

impl<'a> MakeWriter<'a> for RuntimeTraceWriter {
    type Writer = RuntimeTraceGuard;

    fn make_writer(&'a self) -> Self::Writer {
        RuntimeTraceGuard {
            file: OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.path)
                .ok(),
        }
    }
}

struct RuntimeTraceGuard {
    file: Option<fs::File>,
}

impl Write for RuntimeTraceGuard {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let _ = io::stdout().write_all(buf);
        if let Some(file) = &mut self.file {
            let _ = file.write_all(buf);
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        let _ = io::stdout().flush();
        if let Some(file) = &mut self.file {
            let _ = file.flush();
        }
        Ok(())
    }
}

fn append_startup_error(config_path: &str, message: &str) {
    if let Ok(config) = AgentConfig::from_path(config_path) {
        append_runtime_log(&config, message);
        return;
    }

    let output_dir = std::path::Path::new("runtime").join("rust-output");
    if fs::create_dir_all(&output_dir).is_err() {
        return;
    }
    let path = output_dir.join("wechat-summary-app.log");
    if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(path) {
        let _ = writeln!(file, "{} {}", Utc::now().to_rfc3339(), message);
    }
}

async fn handle_platform_event(
    config: &AgentConfig,
    store: &SqliteStateStore,
    matcher: &TriggerMatcher,
    client: &PlatformClient,
    recent_trigger_attempts: &mut RecentTriggerAttempts,
    recent_observed_messages: &mut RecentObservedMessages,
    event: PlatformEvent,
) -> Result<()> {
    let event_preview = event.content.chars().take(40).collect::<String>();
    info!(
        room_id = ?event.room_id,
        content_len = event.content.chars().count(),
        content_preview = %event_preview,
        "platform event received"
    );
    append_runtime_log(
        config,
        &format!(
            "event room={:?} len={} preview={:?}",
            event.room_id,
            event.content.chars().count(),
            event_preview
        ),
    );
    let incoming = IncomingMessage::from(event);
    recent_observed_messages.record(&incoming, Utc::now());
    let Some(trigger) = matcher.match_message(&incoming) else {
        append_runtime_log(
            config,
            &format!(
                "event did not match trigger room={} content={:?}",
                incoming.room_id, incoming.content
            ),
        );
        return Ok(());
    };

    if recent_trigger_attempts.is_duplicate(&trigger, Utc::now()) {
        info!(
            room_id = %trigger.room_id,
            trigger_content = %trigger.trigger_content,
            dedupe_window_seconds = TRIGGER_DEDUPE_WINDOW_SECONDS,
            "duplicate trigger ignored"
        );
        append_runtime_log(
            config,
            &format!(
                "duplicate trigger ignored room={} content={:?} window_seconds={}",
                trigger.room_id, trigger.trigger_content, TRIGGER_DEDUPE_WINDOW_SECONDS
            ),
        );
        return Ok(());
    }

    let command = parse_summary_command(&trigger, client.kind());
    if !client.supports(command.target_platform) {
        let message = format!(
            "暂不支持从 {} 总结 {} 平台消息。当前已接入的平台：{}。",
            client.kind().as_str(),
            command.target_platform.as_str(),
            client.kind().as_str()
        );
        append_runtime_log(
            config,
            &format!(
                "unsupported target platform room={} source_platform={} target_platform={}",
                trigger.room_id,
                client.kind().as_str(),
                command.target_platform.as_str()
            ),
        );
        let _ = client.send_text(&trigger.room_id, &message).await;
        return Ok(());
    }

    let last_trigger = store.get_last_trigger(&trigger.room_id)?;
    if let Some(remaining) = rate_limit_remaining(incoming.timestamp, last_trigger, config) {
        let message = format!(
            "距离上次成功总结还不到 {}，请稍后再试。",
            format_duration_zh(remaining)
        );
        info!(
            room_id = %trigger.room_id,
            remaining_seconds = remaining.num_seconds(),
            "trigger rejected by successful-request rate limit"
        );
        append_runtime_log(
            config,
            &format!(
                "trigger rate limited room={} remaining_seconds={}",
                trigger.room_id,
                remaining.num_seconds()
            ),
        );
        let _ = client.send_text(&trigger.room_id, &message).await;
        return Ok(());
    }

    let mut pipeline_options = PipelineOptions::manual(config, command.image_token_present);
    if pipeline_options.image_gen_enabled {
        let last_image = store.get_last_image(&trigger.room_id)?;
        if let Some(remaining) = image_cooldown_remaining(incoming.timestamp, last_image, config) {
            pipeline_options.image_gen_enabled = false;
            let message = format!(
                "图片生成冷却中，剩余 {}，本次只生成文字总结。",
                format_duration_zh(remaining)
            );
            info!(
                room_id = %trigger.room_id,
                remaining_seconds = remaining.num_seconds(),
                "manual image generation skipped by image cooldown"
            );
            append_runtime_log(
                config,
                &format!(
                    "manual image cooldown active room={} remaining_seconds={}",
                    trigger.room_id,
                    remaining.num_seconds()
                ),
            );
            let _ = client.send_text(&trigger.room_id, &message).await;
        }
    }

    let range = TimeRangeCalculator::resolve_with_override(
        incoming.timestamp,
        last_trigger,
        &config.time_range,
        command.range_minutes,
    );

    info!(
        room_id = %trigger.room_id,
        source_platform = client.kind().as_str(),
        target_platform = command.target_platform.as_str(),
        since = %range.since,
        until = %range.until,
        command_range_minutes = ?command.range_minutes,
        image_token_present = command.image_token_present,
        "trigger accepted; running summary pipeline"
    );
    append_runtime_log(
        config,
        &format!(
            "trigger accepted room={} source_platform={} target_platform={} since={} until={} command_range_minutes={:?} image_token_present={}",
            trigger.room_id,
            client.kind().as_str(),
            command.target_platform.as_str(),
            range.since,
            range.until,
            command.range_minutes,
            command.image_token_present
        ),
    );

    match run_summary_pipeline(
        config,
        client,
        &incoming,
        &trigger,
        &range,
        pipeline_options,
        Some(ImageCooldownRecorder {
            store: store.clone(),
            timestamp: incoming.timestamp,
        }),
        Some(recent_observed_messages),
    )
    .await
    {
        Ok(PipelineOutcome::SummaryProduced) => {
            store.set_last_trigger(&trigger.room_id, incoming.timestamp)?;
            info!(room_id = %trigger.room_id, "summary pipeline completed");
            append_runtime_log(
                config,
                &format!("pipeline completed room={}", trigger.room_id),
            );
        }
        Ok(PipelineOutcome::NoSummary) => {
            info!(room_id = %trigger.room_id, "summary pipeline completed without summary output");
            append_runtime_log(
                config,
                &format!(
                    "pipeline completed without summary room={}",
                    trigger.room_id
                ),
            );
        }
        Err(error) => {
            let error_message = format_error_chain(&error);
            error!(room_id = %trigger.room_id, error = %error_message, "summary pipeline failed");
            append_runtime_log(
                config,
                &format!(
                    "pipeline failed room={} error={}",
                    trigger.room_id, error_message
                ),
            );
            let _ = client
                .send_text(&trigger.room_id, &format!("总结失败：{error_message}"))
                .await;
        }
    }

    Ok(())
}

#[derive(Debug, Default)]
struct RecentTriggerAttempts {
    seen_at_by_key: HashMap<String, DateTime<Utc>>,
}

impl RecentTriggerAttempts {
    fn is_duplicate(&mut self, trigger: &TriggerMatch, now: DateTime<Utc>) -> bool {
        let cutoff = now - Duration::seconds(TRIGGER_DEDUPE_WINDOW_SECONDS);
        self.seen_at_by_key.retain(|_, seen_at| *seen_at >= cutoff);

        let key = trigger_attempt_key(trigger);
        if self
            .seen_at_by_key
            .get(&key)
            .is_some_and(|seen_at| *seen_at >= cutoff)
        {
            return true;
        }

        self.seen_at_by_key.insert(key, now);
        false
    }
}

fn trigger_attempt_key(trigger: &TriggerMatch) -> String {
    format!(
        "{}\n{}",
        trigger.room_id.trim(),
        trigger.trigger_content.trim()
    )
}

#[derive(Debug, Default)]
struct RecentObservedMessages {
    messages: VecDeque<IncomingMessage>,
}

impl RecentObservedMessages {
    fn record(&mut self, message: &IncomingMessage, now: DateTime<Utc>) {
        if message.msg_type != "text" || message.content.trim().is_empty() {
            return;
        }

        self.messages.push_back(message.clone());
        self.prune(now);
    }

    fn count_user_text_in_range(
        &self,
        room_id: &str,
        since: DateTime<Utc>,
        until: DateTime<Utc>,
        incoming: &IncomingMessage,
    ) -> usize {
        self.messages
            .iter()
            .filter(|message| {
                message.room_id == room_id
                    && message.timestamp >= since
                    && message.timestamp <= until
                    && message.msg_type == "text"
                    && !message.is_self
                    && !message.content.trim().is_empty()
                    && !is_current_incoming_message(message, incoming)
                    && !is_agent_status_content(&message.content)
            })
            .count()
    }

    fn prune(&mut self, now: DateTime<Utc>) {
        let cutoff = now - Duration::hours(RECENT_OBSERVED_WINDOW_HOURS);
        self.messages.retain(|message| message.timestamp >= cutoff);
        while self.messages.len() > RECENT_OBSERVED_MAX_MESSAGES {
            self.messages.pop_front();
        }
    }
}

async fn run_scheduled_summaries(
    config: &AgentConfig,
    store: &SqliteStateStore,
    client: &PlatformClient,
    now: DateTime<Utc>,
) -> Result<()> {
    let rooms = scheduled_rooms(config, &client.configured_rooms(config));
    if rooms.is_empty() {
        warn!("scheduled summary skipped because no rooms are configured");
        append_runtime_log(config, "scheduled summary skipped no rooms");
        return Ok(());
    }

    let range = ResolvedTimeRange {
        since: now - Duration::hours(config.scheduled_summary.range_hours.max(1)),
        until: now,
        mode: TimeRangeMode::FixedHours,
    };
    info!(
        rooms = ?rooms,
        since = %range.since,
        until = %range.until,
        "scheduled summary due; running pipeline"
    );
    append_runtime_log(
        config,
        &format!(
            "scheduled summary due rooms={:?} since={} until={}",
            rooms, range.since, range.until
        ),
    );

    for room in rooms {
        if !config.scheduled_summary.ignore_rate_limit {
            let last_trigger = store.get_last_trigger(&room)?;
            if let Some(remaining) = rate_limit_remaining(now, last_trigger, config) {
                info!(
                    room_id = %room,
                    remaining_seconds = remaining.num_seconds(),
                    "scheduled summary skipped by successful-request rate limit"
                );
                append_runtime_log(
                    config,
                    &format!(
                        "scheduled summary rate limited room={} remaining_seconds={}",
                        room,
                        remaining.num_seconds()
                    ),
                );
                continue;
            }
        }

        let incoming = IncomingMessage {
            room_id: room.clone(),
            room_name: Some(room.clone()),
            sender_id: "scheduled_summary".to_string(),
            sender_name: Some("定时总结".to_string()),
            content: "[scheduled_summary]".to_string(),
            msg_type: "text".to_string(),
            timestamp: now,
            is_self: true,
        };
        let trigger = TriggerMatch {
            room_id: room.clone(),
            trigger_symbol: "[scheduled_summary]".to_string(),
            trigger_content: "[scheduled_summary]".to_string(),
        };

        match run_summary_pipeline(
            config,
            client,
            &incoming,
            &trigger,
            &range,
            PipelineOptions::scheduled(config),
            None,
            None,
        )
        .await
        {
            Ok(PipelineOutcome::SummaryProduced) => {
                store.set_last_trigger(&room, now)?;
                info!(room_id = %room, "scheduled summary pipeline completed");
                append_runtime_log(
                    config,
                    &format!("scheduled pipeline completed room={}", room),
                );
            }
            Ok(PipelineOutcome::NoSummary) => {
                info!(room_id = %room, "scheduled summary pipeline completed without summary output");
                append_runtime_log(
                    config,
                    &format!("scheduled pipeline completed without summary room={}", room),
                );
            }
            Err(error) => {
                let error_message = format_error_chain(&error);
                error!(room_id = %room, error = %error_message, "scheduled summary pipeline failed");
                append_runtime_log(
                    config,
                    &format!(
                        "scheduled pipeline failed room={} error={}",
                        room, error_message
                    ),
                );
                let _ = client
                    .send_text(&room, &format!("定时总结失败：{error_message}"))
                    .await;
            }
        }
    }

    Ok(())
}

#[derive(Debug, Clone, Copy)]
struct PipelineOptions {
    text_summary_enabled: bool,
    image_gen_enabled: bool,
    send_progress: bool,
    defer_text_until_image_ready: bool,
    send_disabled_message: bool,
}

#[derive(Clone)]
struct ImageCooldownRecorder {
    store: SqliteStateStore,
    timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum PipelineOutcome {
    SummaryProduced,
    NoSummary,
}

impl PipelineOptions {
    fn manual(config: &AgentConfig, image_token_present: bool) -> Self {
        let image_enabled_for_request =
            config.manual_summary.image_by_default ^ image_token_present;
        Self {
            text_summary_enabled: config.text_summary.enabled,
            image_gen_enabled: config.image_gen.enabled && image_enabled_for_request,
            send_progress: true,
            defer_text_until_image_ready: false,
            send_disabled_message: true,
        }
    }

    fn scheduled(config: &AgentConfig) -> Self {
        Self {
            text_summary_enabled: config.scheduled_summary.send_text && config.text_summary.enabled,
            image_gen_enabled: config.scheduled_summary.send_image && config.image_gen.enabled,
            send_progress: false,
            defer_text_until_image_ready: true,
            send_disabled_message: false,
        }
    }
}

async fn run_summary_pipeline(
    config: &AgentConfig,
    client: &PlatformClient,
    incoming: &IncomingMessage,
    trigger: &TriggerMatch,
    range: &ResolvedTimeRange,
    options: PipelineOptions,
    image_cooldown_recorder: Option<ImageCooldownRecorder>,
    recent_observed_messages: Option<&RecentObservedMessages>,
) -> Result<PipelineOutcome> {
    if !options.text_summary_enabled && !options.image_gen_enabled {
        if options.send_disabled_message {
            client
                .send_text(&trigger.room_id, "当前配置未开启文字总结或图片生成。")
                .await
                .context("sending disabled pipeline message")?;
        }
        return Ok(PipelineOutcome::NoSummary);
    }

    if options.send_progress {
        client
            .send_text(&trigger.room_id, progress_message(options))
            .await
            .context("sending progress message")?;
    }

    if cloud_blocked(config, &trigger.room_id) {
        client
            .send_text(
                &trigger.room_id,
                "当前群聊按配置禁止发送到云端模型，已停止总结。",
            )
            .await
            .context("sending privacy block message")?;
        return Ok(PipelineOutcome::NoSummary);
    }

    let retry_notifier = retry_message_notifier(config, client.sender(), trigger.room_id.clone());

    let history_message_limit = config.history_message_limit();
    let history_query_limit = history_message_limit.min(u32::MAX as usize) as u32;
    let media_decode_limit = image_caption_media_decode_limit(config);
    info!(
        room_id = %trigger.room_id,
        since = %range.since,
        until = %range.until,
        limit = history_message_limit,
        media_decode_limit = ?media_decode_limit,
        "querying platform history"
    );
    append_runtime_log(
        config,
        &format!(
            "history query started room={} since={} until={} limit={} media_decode_limit={}",
            trigger.room_id,
            range.since,
            range.until,
            history_message_limit,
            format_media_decode_limit(media_decode_limit)
        ),
    );
    let mut history = client
        .query_text_messages(
            &trigger.room_id,
            incoming.room_name.as_deref(),
            range.since,
            range.until,
            history_query_limit,
            media_decode_limit,
        )
        .await
        .context("querying platform chat history")?;
    if history.is_empty() {
        let observed_count = recent_observed_messages
            .map(|recent| {
                recent.count_user_text_in_range(
                    &trigger.room_id,
                    range.since,
                    range.until,
                    incoming,
                )
            })
            .unwrap_or(0);
        if observed_count > 0 {
            warn!(
                room_id = %trigger.room_id,
                observed_count,
                since = %range.since,
                until = %range.until,
                "platform history returned empty despite recent observed messages"
            );
            append_runtime_log(
                config,
                &format!(
                    "history empty but recent listener saw messages room={} observed_count={} since={} until={}",
                    trigger.room_id, observed_count, range.since, range.until
                ),
            );
            for (retry_index, delay_ms) in EMPTY_HISTORY_RETRY_DELAYS_MS.iter().copied().enumerate()
            {
                append_runtime_log(
                    config,
                    &format!(
                        "history empty retry scheduled room={} retry={} delay_ms={}",
                        trigger.room_id,
                        retry_index + 1,
                        delay_ms
                    ),
                );
                tokio::time::sleep(StdDuration::from_millis(delay_ms)).await;
                history = client
                    .query_text_messages(
                        &trigger.room_id,
                        incoming.room_name.as_deref(),
                        range.since,
                        range.until,
                        history_query_limit,
                        media_decode_limit,
                    )
                    .await
                    .context("retrying platform chat history after suspicious empty result")?;
                append_runtime_log(
                    config,
                    &format!(
                        "history empty retry completed room={} retry={} history_len={}",
                        trigger.room_id,
                        retry_index + 1,
                        history.len()
                    ),
                );
                if !history.is_empty() {
                    break;
                }
            }

            if history.is_empty() {
                client
                    .send_text(
                        &trigger.room_id,
                        "历史读取暂时为空，但刚刚监听到该群有消息。wxdb 可能还在同步，请稍后再试。",
                    )
                    .await
                    .context("sending suspicious empty-history message")?;
                append_runtime_log(
                    config,
                    &format!(
                        "history suspicious empty after retries room={} observed_count={}",
                        trigger.room_id, observed_count
                    ),
                );
                return Ok(PipelineOutcome::NoSummary);
            }
        }
    }
    let platform_history_len = history.len();
    let first_platform_ts = history.iter().map(|message| message.timestamp).min();
    let last_platform_ts = history.iter().map(|message| message.timestamp).max();
    info!(
        room_id = %trigger.room_id,
        history_len = platform_history_len,
        since = %range.since,
        until = %range.until,
        first_ts = ?first_platform_ts,
        last_ts = ?last_platform_ts,
        "platform history query completed"
    );
    append_runtime_log(
        config,
        &format!(
            "history query completed room={} history_len={} since={} until={} first={} last={}",
            trigger.room_id,
            platform_history_len,
            range.since,
            range.until,
            first_platform_ts
                .map(|timestamp| timestamp.to_string())
                .unwrap_or_else(|| "-".to_string()),
            last_platform_ts
                .map(|timestamp| timestamp.to_string())
                .unwrap_or_else(|| "-".to_string())
        ),
    );
    let raw_history_len = history.len();
    if let Some(first_ts) = first_platform_ts {
        let late_by = first_ts - range.since;
        if late_by > Duration::hours(1) {
            warn!(
                room_id = %trigger.room_id,
                requested_since = %range.since,
                first_ts = %first_ts,
                late_by_minutes = late_by.num_minutes(),
                "platform history starts later than requested window"
            );
            append_runtime_log(
                config,
                &format!(
                    "history coverage starts late room={} requested_since={} first={} missing_before_minutes={}",
                    trigger.room_id,
                    range.since,
                    first_ts,
                    late_by.num_minutes()
                ),
            );
        }
    }
    history.retain(|message| {
        !is_current_trigger_message(message, incoming) && !is_agent_status_message(message)
    });
    let filtered_history_len = history.len();
    let removed_history_len = raw_history_len.saturating_sub(filtered_history_len);
    info!(
        room_id = %trigger.room_id,
        history_len = filtered_history_len,
        raw_history_len,
        removed_history_len,
        "history after trigger-message filtering"
    );
    append_runtime_log(
        config,
        &format!(
            "history after filtering room={} raw_len={} filtered_len={} removed={}",
            trigger.room_id, raw_history_len, filtered_history_len, removed_history_len
        ),
    );

    let image_caption_count = apply_image_captions(config, &trigger.room_id, &mut history).await?;

    let chat_messages = history
        .into_iter()
        .map(history_to_chat_message)
        .collect::<Vec<_>>();
    let formatted = ChatFormatter::format(&chat_messages);
    if formatted.total_messages == 0 {
        client
            .send_text(&trigger.room_id, "这段时间没有可总结的文本聊天记录。")
            .await
            .context("sending empty-history message")?;
        return Ok(PipelineOutcome::NoSummary);
    }

    let privacy = PrivacyFilter::new(config.privacy.clone());
    let llm_input = privacy.apply(&formatted.merged_input);
    info!(
        room_id = %trigger.room_id,
        input_chars = llm_input.chars().count(),
        total_messages = formatted.total_messages,
        text_summary_enabled = options.text_summary_enabled,
        image_gen_enabled = options.image_gen_enabled,
        "LLM input prepared"
    );
    append_runtime_log(
        config,
        &format!(
            "llm input prepared room={} input_chars={} messages={} text_enabled={} image_enabled={}",
            trigger.room_id,
            llm_input.chars().count(),
            formatted.total_messages,
            options.text_summary_enabled,
            options.image_gen_enabled
        ),
    );
    if image_caption_count > 0 {
        append_runtime_log(
            config,
            &format!(
                "image captions inserted room={} count={}",
                trigger.room_id, image_caption_count
            ),
        );
    }
    let llm = OpenAiCompatibleLlm::new(config.llm.clone(), &config.proxy)
        .context("initializing LLM client")?
        .with_retry_notifier(retry_notifier.clone());
    let mut pending_text_reply = None;
    if options.text_summary_enabled {
        let summary_result = complete_chat_summary_with_fallback(
            config,
            &llm,
            &trigger.room_id,
            "text summary",
            &config.text_summary.system_prompt,
            &config.text_summary.user_prompt_template,
            &chat_messages,
            &privacy,
        )
        .await
        .context("calling LLM for text summary")?;
        let summary = summary_result.output;
        info!(
            room_id = %trigger.room_id,
            output_chars = summary.chars().count(),
            "LLM text summary completed"
        );
        append_runtime_log(
            config,
            &format!(
                "llm text summary completed room={} output_chars={}",
                trigger.room_id,
                summary.chars().count()
            ),
        );
        let reply = format_summary_reply(&summary, range, formatted.total_messages);
        if options.defer_text_until_image_ready {
            pending_text_reply = Some(reply);
        } else {
            client
                .send_text(&trigger.room_id, &reply)
                .await
                .context("sending summary text")?;
            info!(room_id = %trigger.room_id, "summary text sent");
            append_runtime_log(
                config,
                &format!("summary text sent room={}", trigger.room_id),
            );
        }
    }

    if options.image_gen_enabled && !options.defer_text_until_image_ready {
        spawn_background_image_pipeline(
            config.clone(),
            client.sender(),
            trigger.room_id.clone(),
            llm_input,
            chat_messages.clone(),
            options.text_summary_enabled,
            image_cooldown_recorder,
        );
        info!(
            room_id = %trigger.room_id,
            "manual image pipeline spawned in background"
        );
        append_runtime_log(
            config,
            &format!("manual image pipeline spawned room={}", trigger.room_id),
        );
        return Ok(PipelineOutcome::SummaryProduced);
    }

    if options.image_gen_enabled {
        let image_summary_result = match complete_chat_summary_with_fallback(
            config,
            &llm,
            &trigger.room_id,
            "image summary",
            &config.image_summary.system_prompt,
            &config.image_summary.user_prompt_template,
            &chat_messages,
            &privacy,
        )
        .await
        .context("calling LLM for image summary")
        {
            Ok(summary) => summary,
            Err(error) => {
                let error_message = format_error_chain(&error);
                warn!(
                    room_id = %trigger.room_id,
                    error = %error_message,
                    "image summary failed after text summary"
                );
                append_runtime_log(
                    config,
                    &format!(
                        "image summary failed after text summary room={} error={}",
                        trigger.room_id, error_message
                    ),
                );
                send_deferred_summary_text(
                    config,
                    client,
                    &trigger.room_id,
                    &mut pending_text_reply,
                    "after image summary failure",
                )
                .await?;
                if options.text_summary_enabled {
                    send_image_failure_message(config, client, &trigger.room_id, &error_message)
                        .await;
                    return Ok(PipelineOutcome::SummaryProduced);
                }
                return Err(error);
            }
        };
        let image_summary = image_summary_result.output;
        let image_prompt_chat_input = chat_input_for_followup_prompt(
            config,
            &config.image_prompt.user_prompt_template,
            &llm_input,
            &image_summary_result.followup_chat_input,
            &image_summary,
        );
        info!(
            room_id = %trigger.room_id,
            output_chars = image_summary.chars().count(),
            "LLM image summary completed"
        );
        append_runtime_log(
            config,
            &format!(
                "llm image summary completed room={} output_chars={}",
                trigger.room_id,
                image_summary.chars().count()
            ),
        );
        let image_prompt_request = render_prompt_template(
            &config.image_prompt.user_prompt_template,
            &image_prompt_chat_input,
            "",
            &image_summary,
        );
        info!(
            room_id = %trigger.room_id,
            prompt_chars = image_prompt_request.chars().count(),
            "calling LLM for image prompt"
        );
        append_runtime_log(
            config,
            &format!(
                "calling llm image prompt room={} prompt_chars={}",
                trigger.room_id,
                image_prompt_request.chars().count()
            ),
        );
        let image_prompt = match llm
            .complete_without_max_tokens(&config.image_prompt.system_prompt, &image_prompt_request)
            .await
            .context("calling LLM for image prompt")
        {
            Ok(prompt) => prompt,
            Err(error) => {
                let error_message = format_error_chain(&error);
                warn!(
                    room_id = %trigger.room_id,
                    error = %error_message,
                    "image prompt failed after text summary"
                );
                append_runtime_log(
                    config,
                    &format!(
                        "image prompt failed after text summary room={} error={}",
                        trigger.room_id, error_message
                    ),
                );
                send_deferred_summary_text(
                    config,
                    client,
                    &trigger.room_id,
                    &mut pending_text_reply,
                    "after image prompt failure",
                )
                .await?;
                if options.text_summary_enabled {
                    send_image_failure_message(config, client, &trigger.room_id, &error_message)
                        .await;
                    return Ok(PipelineOutcome::SummaryProduced);
                }
                return Err(error);
            }
        };
        info!(
            room_id = %trigger.room_id,
            output_chars = image_prompt.chars().count(),
            "LLM image prompt completed"
        );
        append_runtime_log(
            config,
            &format!(
                "llm image prompt completed room={} output_chars={}",
                trigger.room_id,
                image_prompt.chars().count()
            ),
        );

        match generate_summary_image(
            config,
            &trigger.room_id,
            &image_prompt,
            Some(retry_notifier.clone()),
        )
        .await
        {
            Ok(artifact) => {
                send_deferred_summary_text(
                    config,
                    client,
                    &trigger.room_id,
                    &mut pending_text_reply,
                    "before image send",
                )
                .await?;
                send_summary_image(config, client, &trigger.room_id, &artifact).await?;
                record_image_cooldown_success(
                    config,
                    image_cooldown_recorder.as_ref(),
                    &trigger.room_id,
                )?;
            }
            Err(error) => {
                let error_message = format_error_chain(&error);
                warn!(error = %error_message, "image generation or sending failed");
                append_runtime_log(
                    config,
                    &format!(
                        "image generation or sending failed room={} error={}",
                        trigger.room_id, error_message
                    ),
                );
                send_deferred_summary_text(
                    config,
                    client,
                    &trigger.room_id,
                    &mut pending_text_reply,
                    "after image generation failure",
                )
                .await?;
                if options.text_summary_enabled {
                    send_image_failure_message(config, client, &trigger.room_id, &error_message)
                        .await;
                    return Ok(PipelineOutcome::SummaryProduced);
                }
                let prefix = if options.text_summary_enabled {
                    "文字总结已完成，但"
                } else {
                    ""
                };
                client
                    .send_text(
                        &trigger.room_id,
                        &format!("{prefix}图片生成失败：{error_message}"),
                    )
                    .await
                    .context("sending image failure message")?;
            }
        }
    }

    send_deferred_summary_text(
        config,
        client,
        &trigger.room_id,
        &mut pending_text_reply,
        "without image",
    )
    .await?;

    Ok(PipelineOutcome::SummaryProduced)
}

fn spawn_background_image_pipeline(
    config: AgentConfig,
    sender: PlatformSender,
    room_id: String,
    llm_input: String,
    chat_messages: Vec<ChatMessage>,
    text_summary_enabled: bool,
    image_cooldown_recorder: Option<ImageCooldownRecorder>,
) {
    tokio::spawn(async move {
        run_background_image_pipeline(
            config,
            sender,
            room_id,
            llm_input,
            chat_messages,
            text_summary_enabled,
            image_cooldown_recorder,
        )
        .await;
    });
}

async fn run_background_image_pipeline(
    config: AgentConfig,
    sender: PlatformSender,
    room_id: String,
    llm_input: String,
    chat_messages: Vec<ChatMessage>,
    text_summary_enabled: bool,
    image_cooldown_recorder: Option<ImageCooldownRecorder>,
) {
    let result = run_background_image_pipeline_inner(
        &config,
        &sender,
        &room_id,
        &llm_input,
        &chat_messages,
        image_cooldown_recorder.as_ref(),
    )
    .await;
    if let Err(error) = result {
        let error_message = format_error_chain(&error);
        warn!(room_id = %room_id, error = %error_message, "background image pipeline failed");
        append_runtime_log(
            &config,
            &format!(
                "background image pipeline failed room={} error={}",
                room_id, error_message
            ),
        );
        let prefix = if text_summary_enabled {
            "文字总结已完成，但"
        } else {
            ""
        };
        if let Err(send_error) = sender
            .send_text(&room_id, &format!("{prefix}图片生成失败：{error_message}"))
            .await
        {
            warn!(
                room_id = %room_id,
                error = %format_error_chain(&send_error),
                "failed to send background image failure message"
            );
        }
    }
}

async fn run_background_image_pipeline_inner(
    config: &AgentConfig,
    sender: &PlatformSender,
    room_id: &str,
    llm_input: &str,
    chat_messages: &[ChatMessage],
    image_cooldown_recorder: Option<&ImageCooldownRecorder>,
) -> Result<()> {
    let retry_notifier = retry_message_notifier(config, sender.clone(), room_id.to_string());
    let llm = OpenAiCompatibleLlm::new(config.llm.clone(), &config.proxy)
        .context("initializing LLM client for background image pipeline")?
        .with_retry_notifier(retry_notifier.clone());
    let privacy = PrivacyFilter::new(config.privacy.clone());
    let image_summary_result = complete_chat_summary_with_fallback(
        config,
        &llm,
        room_id,
        "background image summary",
        &config.image_summary.system_prompt,
        &config.image_summary.user_prompt_template,
        chat_messages,
        &privacy,
    )
    .await
    .context("calling LLM for background image summary")?;
    let image_summary = image_summary_result.output;
    info!(
        room_id = %room_id,
        output_chars = image_summary.chars().count(),
        "LLM background image summary completed"
    );
    append_runtime_log(
        config,
        &format!(
            "llm background image summary completed room={} output_chars={}",
            room_id,
            image_summary.chars().count()
        ),
    );

    let image_prompt_chat_input = chat_input_for_followup_prompt(
        config,
        &config.image_prompt.user_prompt_template,
        llm_input,
        &image_summary_result.followup_chat_input,
        &image_summary,
    );
    let image_prompt_request = render_prompt_template(
        &config.image_prompt.user_prompt_template,
        &image_prompt_chat_input,
        "",
        &image_summary,
    );
    info!(
        room_id = %room_id,
        prompt_chars = image_prompt_request.chars().count(),
        "calling LLM for background image prompt"
    );
    append_runtime_log(
        config,
        &format!(
            "calling llm background image prompt room={} prompt_chars={}",
            room_id,
            image_prompt_request.chars().count()
        ),
    );
    let image_prompt = llm
        .complete_without_max_tokens(&config.image_prompt.system_prompt, &image_prompt_request)
        .await
        .context("calling LLM for background image prompt")?;
    info!(
        room_id = %room_id,
        output_chars = image_prompt.chars().count(),
        "LLM background image prompt completed"
    );
    append_runtime_log(
        config,
        &format!(
            "llm background image prompt completed room={} output_chars={}",
            room_id,
            image_prompt.chars().count()
        ),
    );

    let artifact =
        generate_summary_image(config, room_id, &image_prompt, Some(retry_notifier)).await?;
    send_summary_image_with_sender(config, sender, room_id, &artifact).await?;
    record_image_cooldown_success(config, image_cooldown_recorder, room_id)?;
    append_runtime_log(
        config,
        &format!("background image pipeline completed room={}", room_id),
    );
    Ok(())
}

#[derive(Debug, Clone)]
struct LongChatCompletion {
    output: String,
    followup_chat_input: String,
}

#[derive(Debug, Clone)]
struct LlmChunkRequest {
    index: usize,
    message_count: usize,
    input_chars: usize,
    prompt_chars: usize,
    prompt: String,
}

#[derive(Debug, Clone)]
struct ChunkSummary {
    index: usize,
    message_count: usize,
    output: String,
}

async fn complete_chat_summary_with_fallback(
    config: &AgentConfig,
    llm: &OpenAiCompatibleLlm,
    room_id: &str,
    stage: &str,
    system_prompt: &str,
    user_prompt_template: &str,
    chat_messages: &[ChatMessage],
    privacy: &PrivacyFilter,
) -> Result<LongChatCompletion> {
    let max_prompt_chars = config.privacy.max_chars_to_llm.max(1);
    let full_chat_input = private_formatted_chat_input(chat_messages, privacy);
    let full_prompt = render_prompt_template(user_prompt_template, &full_chat_input, "", "");
    let full_prompt_chars = full_prompt.chars().count();
    if full_prompt_chars <= max_prompt_chars || chat_messages.len() <= 1 {
        append_runtime_log(
            config,
            &format!(
                "calling llm {} room={} prompt_chars={} mode=direct",
                stage, room_id, full_prompt_chars
            ),
        );
        let output = complete_llm_with_rate_limit_queue(
            config,
            llm,
            room_id,
            stage,
            system_prompt,
            full_prompt,
        )
        .await?;
        return Ok(LongChatCompletion {
            output,
            followup_chat_input: full_chat_input,
        });
    }

    let chunks = build_llm_chunk_requests(
        chat_messages,
        privacy,
        user_prompt_template,
        max_prompt_chars,
    );
    info!(
        room_id = %room_id,
        stage,
        prompt_chars = full_prompt_chars,
        max_prompt_chars,
        chunks = chunks.len(),
        "LLM long chat fallback activated"
    );
    append_runtime_log(
        config,
        &format!(
            "llm long chat fallback room={} stage={} prompt_chars={} max_chars={} chunks={}",
            room_id,
            stage,
            full_prompt_chars,
            max_prompt_chars,
            chunks.len()
        ),
    );
    let chunk_summaries =
        complete_chunk_requests(config, llm, room_id, stage, system_prompt, &chunks).await?;
    let combined_input = format_chunk_summaries_for_final(&chunk_summaries);
    let final_prompt = render_prompt_template(user_prompt_template, &combined_input, "", "");
    let final_prompt_chars = final_prompt.chars().count();
    if final_prompt_chars > max_prompt_chars {
        warn!(
            room_id = %room_id,
            stage,
            final_prompt_chars,
            max_prompt_chars,
            chunks = chunk_summaries.len(),
            "LLM long chat final summary still exceeds limit; returning concatenated chunk summaries"
        );
        append_runtime_log(
            config,
            &format!(
                "llm long chat final skipped room={} stage={} final_prompt_chars={} max_chars={} chunks={}",
                room_id,
                stage,
                final_prompt_chars,
                max_prompt_chars,
                chunk_summaries.len()
            ),
        );
        return Ok(LongChatCompletion {
            output: combined_input.clone(),
            followup_chat_input: combined_input,
        });
    }

    append_runtime_log(
        config,
        &format!(
            "calling llm {} final room={} prompt_chars={} chunks={}",
            stage,
            room_id,
            final_prompt_chars,
            chunk_summaries.len()
        ),
    );
    let output = complete_llm_with_rate_limit_queue(
        config,
        llm,
        room_id,
        &format!("{stage} final"),
        system_prompt,
        final_prompt,
    )
    .await?;
    Ok(LongChatCompletion {
        output,
        followup_chat_input: combined_input,
    })
}

async fn complete_chunk_requests(
    config: &AgentConfig,
    llm: &OpenAiCompatibleLlm,
    room_id: &str,
    stage: &str,
    system_prompt: &str,
    chunks: &[LlmChunkRequest],
) -> Result<Vec<ChunkSummary>> {
    let mut join_set = JoinSet::new();
    for chunk in chunks.iter().cloned() {
        append_runtime_log(
            config,
            &format!(
                "llm chunk scheduled room={} stage={} chunk={}/{} message_count={} input_chars={} prompt_chars={}",
                room_id,
                stage,
                chunk.index + 1,
                chunks.len(),
                chunk.message_count,
                chunk.input_chars,
                chunk.prompt_chars
            ),
        );
        let llm = llm.clone();
        let system_prompt = system_prompt.to_string();
        join_set.spawn(async move {
            let result = llm.complete(&system_prompt, &chunk.prompt).await;
            (chunk, result)
        });
    }

    let mut summaries = vec![None; chunks.len()];
    let mut rate_limited = Vec::new();
    let mut first_error = None;
    while let Some(joined) = join_set.join_next().await {
        let (chunk, result) = joined.context("joining LLM chunk request task")?;
        match result {
            Ok(output) => {
                append_runtime_log(
                    config,
                    &format!(
                        "llm chunk completed room={} stage={} chunk={}/{} message_count={} output_chars={}",
                        room_id,
                        stage,
                        chunk.index + 1,
                        chunks.len(),
                        chunk.message_count,
                        output.chars().count()
                    ),
                );
                summaries[chunk.index] = Some(ChunkSummary {
                    index: chunk.index,
                    message_count: chunk.message_count,
                    output,
                });
            }
            Err(AiError::RateLimited(message)) => {
                warn!(
                    room_id = %room_id,
                    stage,
                    chunk = chunk.index + 1,
                    total_chunks = chunks.len(),
                    error = %message,
                    "LLM chunk hit rate limit; queueing retry"
                );
                append_runtime_log(
                    config,
                    &format!(
                        "llm chunk rate limited room={} stage={} chunk={}/{} error={}",
                        room_id,
                        stage,
                        chunk.index + 1,
                        chunks.len(),
                        message
                    ),
                );
                rate_limited.push(chunk);
            }
            Err(error) => {
                if first_error.is_none() {
                    first_error = Some((chunk.index, error));
                }
            }
        }
    }

    if let Some((index, error)) = first_error {
        return Err(anyhow::Error::new(error)
            .context(format!("calling LLM for {stage} chunk {}", index + 1)));
    }

    rate_limited.sort_by_key(|chunk| chunk.index);
    for chunk in rate_limited {
        append_runtime_log(
            config,
            &format!(
                "llm chunk queued retry waiting room={} stage={} chunk={}/{} delay_seconds={}",
                room_id,
                stage,
                chunk.index + 1,
                chunks.len(),
                LLM_RATE_LIMIT_QUEUE_DELAY_SECONDS
            ),
        );
        tokio::time::sleep(StdDuration::from_secs(LLM_RATE_LIMIT_QUEUE_DELAY_SECONDS)).await;
        let output = complete_llm_with_rate_limit_queue(
            config,
            llm,
            room_id,
            &format!("{stage} chunk {}", chunk.index + 1),
            system_prompt,
            chunk.prompt.clone(),
        )
        .await?;
        summaries[chunk.index] = Some(ChunkSummary {
            index: chunk.index,
            message_count: chunk.message_count,
            output,
        });
    }

    summaries
        .into_iter()
        .enumerate()
        .map(|(index, summary)| {
            summary.with_context(|| format!("missing LLM chunk summary {}", index + 1))
        })
        .collect()
}

async fn complete_llm_with_rate_limit_queue(
    config: &AgentConfig,
    llm: &OpenAiCompatibleLlm,
    room_id: &str,
    stage: &str,
    system_prompt: &str,
    prompt: String,
) -> Result<String> {
    for attempt in 1..=LLM_RATE_LIMIT_QUEUE_MAX_ATTEMPTS {
        match llm.complete(system_prompt, &prompt).await {
            Ok(output) => return Ok(output),
            Err(AiError::RateLimited(message)) if attempt < LLM_RATE_LIMIT_QUEUE_MAX_ATTEMPTS => {
                warn!(
                    room_id = %room_id,
                    stage,
                    attempt,
                    max_attempts = LLM_RATE_LIMIT_QUEUE_MAX_ATTEMPTS,
                    delay_seconds = LLM_RATE_LIMIT_QUEUE_DELAY_SECONDS,
                    error = %message,
                    "LLM request rate limited; queued retry waiting"
                );
                append_runtime_log(
                    config,
                    &format!(
                        "llm rate limited queued retry room={} stage={} attempt={} delay_seconds={} error={}",
                        room_id,
                        stage,
                        attempt,
                        LLM_RATE_LIMIT_QUEUE_DELAY_SECONDS,
                        message
                    ),
                );
                tokio::time::sleep(StdDuration::from_secs(LLM_RATE_LIMIT_QUEUE_DELAY_SECONDS))
                    .await;
            }
            Err(error) => return Err(anyhow::Error::new(error)),
        }
    }

    bail!(
        "LLM request for {stage} stayed rate limited after {} queued attempts",
        LLM_RATE_LIMIT_QUEUE_MAX_ATTEMPTS
    )
}

fn build_llm_chunk_requests(
    messages: &[ChatMessage],
    privacy: &PrivacyFilter,
    user_prompt_template: &str,
    max_prompt_chars: usize,
) -> Vec<LlmChunkRequest> {
    let mut sorted = messages.to_vec();
    sorted.sort_by_key(|message| message.timestamp);
    let prompt_overhead = render_prompt_template(user_prompt_template, "", "", "")
        .chars()
        .count();
    let line_budget = max_prompt_chars
        .saturating_sub(prompt_overhead)
        .saturating_sub(CHUNK_PROMPT_HEADROOM_CHARS)
        .max(1);

    let mut rough_chunks = Vec::<Vec<ChatMessage>>::new();
    let mut current = Vec::<ChatMessage>::new();
    let mut current_chars = 0usize;
    for message in sorted {
        let line_chars = formatted_chat_line_chars(&message);
        if !current.is_empty()
            && current_chars.saturating_add(line_chars).saturating_add(1) > line_budget
        {
            rough_chunks.push(std::mem::take(&mut current));
            current_chars = 0;
        }
        current_chars = current_chars.saturating_add(line_chars).saturating_add(1);
        current.push(message);
    }
    if !current.is_empty() {
        rough_chunks.push(current);
    }

    let mut fitted_chunks = Vec::<Vec<ChatMessage>>::new();
    for chunk in rough_chunks {
        push_fitted_llm_chunks(
            chunk,
            privacy,
            user_prompt_template,
            max_prompt_chars,
            &mut fitted_chunks,
        );
    }

    fitted_chunks
        .into_iter()
        .enumerate()
        .map(|(index, chunk_messages)| {
            let input = private_formatted_chat_input(&chunk_messages, privacy);
            let prompt = render_prompt_template(user_prompt_template, &input, "", "");
            LlmChunkRequest {
                index,
                message_count: chunk_messages.len(),
                input_chars: input.chars().count(),
                prompt_chars: prompt.chars().count(),
                prompt,
            }
        })
        .collect()
}

fn push_fitted_llm_chunks(
    chunk: Vec<ChatMessage>,
    privacy: &PrivacyFilter,
    user_prompt_template: &str,
    max_prompt_chars: usize,
    output: &mut Vec<Vec<ChatMessage>>,
) {
    if chunk.len() <= 1 {
        output.push(chunk);
        return;
    }

    let input = private_formatted_chat_input(&chunk, privacy);
    let prompt_chars = render_prompt_template(user_prompt_template, &input, "", "")
        .chars()
        .count();
    if prompt_chars <= max_prompt_chars {
        output.push(chunk);
        return;
    }

    let midpoint = chunk.len() / 2;
    let right = chunk[midpoint..].to_vec();
    let left = chunk[..midpoint].to_vec();
    push_fitted_llm_chunks(
        left,
        privacy,
        user_prompt_template,
        max_prompt_chars,
        output,
    );
    push_fitted_llm_chunks(
        right,
        privacy,
        user_prompt_template,
        max_prompt_chars,
        output,
    );
}

fn private_formatted_chat_input(messages: &[ChatMessage], privacy: &PrivacyFilter) -> String {
    privacy.apply(&ChatFormatter::format(messages).merged_input)
}

fn formatted_chat_line_chars(message: &ChatMessage) -> usize {
    format!(
        "[{}] {}: {}",
        format_beijing_time(message.timestamp),
        message.display_sender(),
        message.content.trim()
    )
    .chars()
    .count()
}

fn format_chunk_summaries_for_final(summaries: &[ChunkSummary]) -> String {
    let mut parts = vec![
        "[CHUNK_SUMMARIES]".to_string(),
        "以下是同一段群聊按时间顺序切分后的分段总结。请综合这些分段总结，输出一份全局总结；如果无法再次请求模型，可直接发送这些分段总结。"
            .to_string(),
    ];
    for summary in summaries {
        parts.push(format!(
            "===== 分段 {}/{}，{} 条 =====\n{}",
            summary.index + 1,
            summaries.len(),
            summary.message_count,
            summary.output.trim()
        ));
    }
    parts.join("\n\n")
}

fn chat_input_for_followup_prompt(
    config: &AgentConfig,
    user_prompt_template: &str,
    full_chat_input: &str,
    fallback_chat_input: &str,
    image_summary: &str,
) -> String {
    let full_prompt_chars =
        render_prompt_template(user_prompt_template, full_chat_input, "", image_summary)
            .chars()
            .count();
    if full_prompt_chars <= config.privacy.max_chars_to_llm {
        return full_chat_input.to_string();
    }

    let fallback_prompt_chars =
        render_prompt_template(user_prompt_template, fallback_chat_input, "", image_summary)
            .chars()
            .count();
    if fallback_prompt_chars <= config.privacy.max_chars_to_llm {
        return fallback_chat_input.to_string();
    }

    format!("[IMAGE_SUMMARY]\n{}", image_summary.trim())
}

async fn generate_summary_image(
    config: &AgentConfig,
    room_id: &str,
    image_prompt: &str,
    retry_notifier: Option<RetryNotifier>,
) -> Result<ImageArtifact> {
    let mut image_client = OpenAiImageClient::new(config.image_gen.clone(), &config.proxy)
        .context("initializing image client")?;
    if let Some(retry_notifier) = retry_notifier {
        image_client = image_client.with_retry_notifier(retry_notifier);
    }
    info!(
        room_id = %room_id,
        prompt_chars = image_prompt.chars().count(),
        "generating summary image"
    );
    append_runtime_log(
        config,
        &format!(
            "generating summary image room={} prompt_chars={}",
            room_id,
            image_prompt.chars().count()
        ),
    );
    let artifact = image_client
        .generate_from_prompt(image_prompt, &config.runtime.output_dir)
        .await
        .context("generating summary image")?;
    info!(
        room_id = %room_id,
        path = %artifact.path,
        size_bytes = artifact.size_bytes,
        "summary image generated"
    );
    append_runtime_log(
        config,
        &format!(
            "summary image generated room={} path={} size_bytes={}",
            room_id, artifact.path, artifact.size_bytes
        ),
    );
    Ok(artifact)
}

async fn send_summary_image_with_sender(
    config: &AgentConfig,
    sender: &PlatformSender,
    room_id: &str,
    artifact: &ImageArtifact,
) -> Result<()> {
    info!(room_id = %room_id, path = %artifact.path, "sending summary image");
    sender
        .send_image(room_id, &artifact.path)
        .await
        .context("sending summary image")?;
    info!(room_id = %room_id, "summary image sent");
    append_runtime_log(config, &format!("summary image sent room={}", room_id));
    Ok(())
}

async fn send_image_failure_message(
    config: &AgentConfig,
    client: &PlatformClient,
    room_id: &str,
    error_message: &str,
) {
    if let Err(error) = client
        .send_text(
            room_id,
            &format!("文字总结已完成，但图片生成失败：{error_message}"),
        )
        .await
    {
        let send_error = format_error_chain(&error);
        warn!(
            room_id = %room_id,
            error = %send_error,
            "failed to send image failure message after completed text summary"
        );
        append_runtime_log(
            config,
            &format!(
                "failed to send image failure message room={} error={}",
                room_id, send_error
            ),
        );
    }
}

async fn send_summary_image(
    config: &AgentConfig,
    client: &PlatformClient,
    room_id: &str,
    artifact: &ImageArtifact,
) -> Result<()> {
    info!(room_id = %room_id, path = %artifact.path, "sending summary image");
    client
        .send_image(room_id, &artifact.path)
        .await
        .context("sending summary image")?;
    info!(room_id = %room_id, "summary image sent");
    append_runtime_log(config, &format!("summary image sent room={}", room_id));
    Ok(())
}

async fn send_deferred_summary_text(
    config: &AgentConfig,
    client: &PlatformClient,
    room_id: &str,
    pending_text_reply: &mut Option<String>,
    reason: &str,
) -> Result<bool> {
    let Some(reply) = pending_text_reply.take() else {
        return Ok(false);
    };

    client
        .send_text(room_id, &reply)
        .await
        .with_context(|| format!("sending deferred summary text {reason}"))?;
    info!(
        room_id = %room_id,
        reason = %reason,
        "deferred summary text sent"
    );
    append_runtime_log(
        config,
        &format!(
            "deferred summary text sent room={} reason={}",
            room_id, reason
        ),
    );
    Ok(true)
}

fn record_image_cooldown_success(
    config: &AgentConfig,
    recorder: Option<&ImageCooldownRecorder>,
    room_id: &str,
) -> Result<()> {
    let Some(recorder) = recorder else {
        return Ok(());
    };

    recorder
        .store
        .set_last_image(room_id, recorder.timestamp)
        .context("recording image cooldown state")?;
    info!(
        room_id = %room_id,
        cooldown_started_at = %recorder.timestamp,
        "image cooldown state recorded"
    );
    append_runtime_log(
        config,
        &format!(
            "image cooldown state recorded room={} cooldown_started_at={}",
            room_id, recorder.timestamp
        ),
    );
    Ok(())
}

fn progress_message(options: PipelineOptions) -> &'static str {
    match (options.text_summary_enabled, options.image_gen_enabled) {
        (true, true) => "收到 /总结，正在整理群聊并生成图片。",
        (true, false) => "收到 /总结，正在整理文字总结。",
        (false, true) => "收到 /总结，正在整理群聊并生成图片。",
        (false, false) => "当前配置未开启文字总结或图片生成。",
    }
}

fn next_scheduled_run_after(now: DateTime<Utc>, config: &AgentConfig) -> Option<DateTime<Utc>> {
    if !config.scheduled_summary.enabled {
        return None;
    }
    if config.scheduled_summary.local_hour > 23 || config.scheduled_summary.local_minute > 59 {
        warn!(
            local_hour = config.scheduled_summary.local_hour,
            local_minute = config.scheduled_summary.local_minute,
            "scheduled summary disabled because local time is invalid"
        );
        return None;
    }

    let local_now = now + Duration::hours(8);
    let local_run_time = local_now.date_naive().and_hms_opt(
        config.scheduled_summary.local_hour,
        config.scheduled_summary.local_minute,
        0,
    )?;
    let run_at = Utc.from_utc_datetime(&local_run_time) - Duration::hours(8);
    if run_at >= now {
        Some(run_at)
    } else {
        Some(run_at + Duration::days(1))
    }
}

fn scheduled_rooms(config: &AgentConfig, platform_rooms: &[String]) -> Vec<String> {
    let rooms = if !config.scheduled_summary.rooms.is_empty() {
        config.scheduled_summary.rooms.clone()
    } else {
        platform_rooms.to_vec()
    };

    rooms
        .into_iter()
        .filter(|room| !room.trim().is_empty())
        .collect()
}

fn render_prompt_template(
    template: &str,
    chat_input: &str,
    text_summary: &str,
    image_summary: &str,
) -> String {
    template
        .replace("{chat_input}", chat_input)
        .replace("{text_summary}", text_summary)
        .replace("{image_summary}", image_summary)
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
struct SummaryCommand {
    target_platform: PlatformKindConfig,
    range_minutes: Option<i64>,
    image_token_present: bool,
}

fn parse_summary_command(
    trigger: &TriggerMatch,
    default_platform: PlatformKindConfig,
) -> SummaryCommand {
    let args = trigger
        .trigger_content
        .strip_prefix(&trigger.trigger_symbol)
        .unwrap_or_default()
        .trim();
    parse_summary_command_args(args, default_platform)
}

fn parse_summary_command_args(args: &str, default_platform: PlatformKindConfig) -> SummaryCommand {
    let args = args.trim();
    if args.is_empty() {
        return SummaryCommand {
            target_platform: default_platform,
            range_minutes: None,
            image_token_present: false,
        };
    }

    let mut target_platform = default_platform;
    let mut image_token_present = false;
    let mut range_tokens = Vec::new();
    for token in args.split_whitespace() {
        if let Some(platform) = parse_platform_token(token) {
            target_platform = platform;
        } else if is_image_token(token) {
            image_token_present = true;
        } else {
            range_tokens.push(token);
        }
    }

    SummaryCommand {
        target_platform,
        range_minutes: parse_command_time_range_minutes(&range_tokens.join(" ")),
        image_token_present,
    }
}

fn parse_platform_token(token: &str) -> Option<PlatformKindConfig> {
    PlatformKindConfig::parse_alias(token)
}

fn is_image_token(token: &str) -> bool {
    let token = token.trim();
    matches!(token, "图片") || matches!(token.to_ascii_lowercase().as_str(), "image" | "img")
}

fn rate_limit_remaining(
    now: DateTime<Utc>,
    last_success: Option<DateTime<Utc>>,
    config: &AgentConfig,
) -> Option<Duration> {
    if !config.rate_limit.enabled || config.rate_limit.successful_request_cooldown_seconds <= 0 {
        return None;
    }

    let last_success = last_success?;
    let cooldown = Duration::seconds(config.rate_limit.successful_request_cooldown_seconds);
    let elapsed = now - last_success;
    (elapsed < cooldown).then_some(cooldown - elapsed)
}

fn image_cooldown_remaining(
    now: DateTime<Utc>,
    last_image_success: Option<DateTime<Utc>>,
    config: &AgentConfig,
) -> Option<Duration> {
    if !config.rate_limit.enabled || config.rate_limit.successful_image_cooldown_seconds <= 0 {
        return None;
    }

    let last_image_success = last_image_success?;
    let summary_cooldown =
        Duration::seconds(config.rate_limit.successful_request_cooldown_seconds.max(0));
    let image_cooldown = Duration::seconds(config.rate_limit.successful_image_cooldown_seconds);
    let cooldown = summary_cooldown + image_cooldown;
    let elapsed = now - last_image_success;
    (elapsed < cooldown).then_some(cooldown - elapsed)
}

fn format_duration_zh(duration: Duration) -> String {
    let seconds = duration.num_seconds().max(1);
    let minutes = seconds / 60;
    let remainder = seconds % 60;
    if minutes <= 0 {
        format!("{seconds}秒")
    } else if remainder == 0 {
        format!("{minutes}分钟")
    } else {
        format!("{minutes}分{remainder}秒")
    }
}

fn cloud_blocked(config: &AgentConfig, room_id: &str) -> bool {
    !config.privacy.cloud_allowed
        && (config.privacy.sensitive_rooms.is_empty()
            || config
                .privacy
                .sensitive_rooms
                .iter()
                .any(|room| room == room_id))
}

fn is_current_trigger_message(
    message: &PlatformHistoryMessage,
    incoming: &IncomingMessage,
) -> bool {
    message.timestamp == incoming.timestamp
        && message.content.trim() == incoming.content.trim()
        && (message.sender_id == incoming.sender_id || message.is_self)
}

fn is_current_incoming_message(message: &IncomingMessage, incoming: &IncomingMessage) -> bool {
    message.timestamp == incoming.timestamp
        && message.content.trim() == incoming.content.trim()
        && message.sender_id == incoming.sender_id
}

fn is_agent_status_message(message: &PlatformHistoryMessage) -> bool {
    is_agent_status_content(&message.content)
}

fn is_agent_status_content(content: &str) -> bool {
    let content = content.trim();
    content.starts_with("收到 /总结")
        || content.starts_with("收到 #总结")
        || content.starts_with("总结失败：")
        || content.starts_with("这段时间没有可总结的文本聊天记录")
        || content.starts_with("历史读取暂时为空")
        || content.starts_with("当前配置未开启文字总结或图片生成")
        || content.starts_with("群聊总结（")
        || content.contains("暂时失败，正在重试")
}

fn format_summary_reply(summary: &str, range: &ResolvedTimeRange, total_messages: usize) -> String {
    format!(
        "群聊总结（{} - {}，{} 条）\n\n{}",
        format_beijing_time(range.since),
        format_beijing_time(range.until),
        total_messages,
        summary.trim()
    )
}

fn format_beijing_time(value: DateTime<Utc>) -> String {
    (value + Duration::hours(8))
        .format("%m-%d %H:%M")
        .to_string()
}

fn format_error_chain(error: &anyhow::Error) -> String {
    error
        .chain()
        .map(|cause| cause.to_string())
        .collect::<Vec<_>>()
        .join(": ")
}

fn image_caption_media_decode_limit(config: &AgentConfig) -> Option<usize> {
    if config.image_caption.enabled {
        Some(config.image_caption.max_images_per_summary)
    } else {
        Some(0)
    }
}

fn format_media_decode_limit(limit: Option<usize>) -> String {
    limit
        .map(|limit| limit.to_string())
        .unwrap_or_else(|| "unlimited".to_string())
}

async fn apply_image_captions(
    config: &AgentConfig,
    room_id: &str,
    history: &mut [PlatformHistoryMessage],
) -> Result<usize> {
    if !config.image_caption.enabled || config.image_caption.max_images_per_summary == 0 {
        return Ok(0);
    }
    let captioner =
        match OpenAiVisionCaptionClient::new(config.image_caption.clone(), &config.proxy) {
            Ok(client) => client,
            Err(error) => {
                let error = error.to_string();
                warn!(
                    room_id = %room_id,
                    error = %error,
                    "image caption client initialization failed; continuing without captions"
                );
                append_runtime_log(
                    config,
                    &format!("image caption init failed room={} error={}", room_id, error),
                );
                return Ok(0);
            }
        };

    let mut inserted = 0usize;
    let mut attempted = 0usize;
    for message in history.iter_mut() {
        if attempted >= config.image_caption.max_images_per_summary {
            break;
        }
        if !is_image_message_type(&message.msg_type) {
            continue;
        }
        let Some(source) = image_caption_source(message) else {
            if let Some(error) = message.media_decode_error.as_deref() {
                append_runtime_log(
                    config,
                    &format!(
                        "image caption skipped room={} reason=decode_failed error={}",
                        room_id, error
                    ),
                );
            }
            continue;
        };
        attempted += 1;
        match captioner.caption_image(&source).await {
            Ok(caption) => {
                let caption = caption.trim();
                if caption.is_empty() {
                    continue;
                }
                message.content = format!("{}（图片转述：{}）", message.content.trim(), caption);
                inserted += 1;
                info!(
                    room_id = %room_id,
                    inserted,
                    attempted,
                    "image caption inserted into history"
                );
            }
            Err(error) => {
                let error = error.to_string();
                warn!(
                    room_id = %room_id,
                    attempted,
                    error = %error,
                    "image caption failed; keeping image placeholder"
                );
                append_runtime_log(
                    config,
                    &format!(
                        "image caption failed room={} attempted={} error={}",
                        room_id, attempted, error
                    ),
                );
                if is_image_caption_auth_error(&error) {
                    warn!(
                        room_id = %room_id,
                        attempted,
                        "image caption stopped after authentication failure"
                    );
                    append_runtime_log(
                        config,
                        &format!(
                            "image caption stopped room={} reason=authentication_failed attempted={}",
                            room_id, attempted
                        ),
                    );
                    break;
                }
            }
        }
    }

    if attempted > 0 {
        append_runtime_log(
            config,
            &format!(
                "image caption completed room={} attempted={} inserted={}",
                room_id, attempted, inserted
            ),
        );
    }
    Ok(inserted)
}

fn is_image_caption_auth_error(error: &str) -> bool {
    let lower = error.to_ascii_lowercase();
    lower.contains("401")
        || lower.contains("unauthorized")
        || lower.contains("invalid_platform_key")
        || lower.contains("missing or invalid platform key")
}

fn image_caption_source(message: &PlatformHistoryMessage) -> Option<String> {
    message
        .decoded_media_path
        .as_deref()
        .filter(|path| !path.trim().is_empty())
        .map(ToOwned::to_owned)
        .or_else(|| {
            message
                .media_path
                .as_deref()
                .filter(|path| {
                    let path = path.trim();
                    path.starts_with("http://")
                        || path.starts_with("https://")
                        || path.starts_with("data:")
                })
                .map(ToOwned::to_owned)
        })
}

fn is_image_message_type(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "image" | "img" | "3" | "图片"
    )
}

fn history_to_chat_message(message: PlatformHistoryMessage) -> ChatMessage {
    ChatMessage {
        timestamp: message.timestamp,
        sender_id: message.sender_id,
        sender_name: message.sender_name,
        content: message.content,
        msg_type: message.msg_type,
    }
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use std::time::Duration as TestDuration;

    use super::*;

    #[test]
    fn current_trigger_message_matches_self_history_row() {
        let timestamp = Utc.timestamp_opt(1_716_464_700, 0).unwrap();
        let history = PlatformHistoryMessage {
            timestamp,
            sender_id: "self".to_string(),
            sender_name: Some("我".to_string()),
            content: "/总结".to_string(),
            msg_type: "text".to_string(),
            media_path: None,
            thumbnail_path: None,
            decoded_media_path: None,
            media_decode_error: None,
            is_self: true,
        };
        let incoming = IncomingMessage {
            room_id: "room@chatroom".to_string(),
            room_name: None,
            sender_id: "wxid_self".to_string(),
            sender_name: None,
            content: "/总结".to_string(),
            msg_type: "text".to_string(),
            timestamp,
            is_self: false,
        };

        assert!(is_current_trigger_message(&history, &incoming));
    }

    #[test]
    fn formats_error_chain_with_root_cause() {
        let error = anyhow::anyhow!("missing environment variable LLM_MODEL for LLM model name")
            .context("initializing LLM client");
        assert_eq!(
            format_error_chain(&error),
            "initializing LLM client: missing environment variable LLM_MODEL for LLM model name"
        );
    }

    #[test]
    fn render_prompt_template_keeps_text_and_image_summaries_separate() {
        let rendered = render_prompt_template(
            "chat={chat_input}; text={text_summary}; image={image_summary}",
            "chat-input",
            "text-result",
            "image-result",
        );

        assert_eq!(
            rendered,
            "chat=chat-input; text=text-result; image=image-result"
        );
    }

    #[test]
    fn detects_agent_status_messages() {
        let message = PlatformHistoryMessage {
            timestamp: Utc::now(),
            sender_id: "self".into(),
            sender_name: None,
            content: "收到 /总结，正在整理文字总结。".into(),
            msg_type: "text".into(),
            media_path: None,
            thumbnail_path: None,
            decoded_media_path: None,
            media_decode_error: None,
            is_self: true,
        };

        assert!(is_agent_status_message(&message));
    }

    #[test]
    fn image_caption_source_prefers_decoded_local_path_and_accepts_urls() {
        let mut message = PlatformHistoryMessage {
            timestamp: Utc::now(),
            sender_id: "u".into(),
            sender_name: None,
            content: "[图片] local_id=1".into(),
            msg_type: "image".into(),
            media_path: Some("https://cdn.example/image.png".into()),
            thumbnail_path: None,
            decoded_media_path: Some(r"D:\Temp\decoded.jpg".into()),
            media_decode_error: None,
            is_self: false,
        };

        assert_eq!(
            image_caption_source(&message).as_deref(),
            Some(r"D:\Temp\decoded.jpg")
        );

        message.decoded_media_path = None;
        assert_eq!(
            image_caption_source(&message).as_deref(),
            Some("https://cdn.example/image.png")
        );

        message.media_path = Some(r"D:\Temp\raw.dat".into());
        assert_eq!(image_caption_source(&message), None);
    }

    #[test]
    fn media_decode_limit_follows_image_caption_config() {
        let mut config = test_config();
        config.image_caption.enabled = false;
        config.image_caption.max_images_per_summary = 20;
        assert_eq!(image_caption_media_decode_limit(&config), Some(0));

        config.image_caption.enabled = true;
        config.image_caption.max_images_per_summary = 7;
        assert_eq!(image_caption_media_decode_limit(&config), Some(7));
        assert_eq!(format_media_decode_limit(Some(7)), "7");
        assert_eq!(format_media_decode_limit(None), "unlimited");
    }

    #[test]
    fn config_reloader_applies_valid_file_changes() {
        let path = unique_config_path();
        write_hot_reload_config(&path, "/总结", 30);
        let mut reloader = ConfigReloader::load(path.to_str().unwrap()).unwrap();
        assert_eq!(reloader.config().time_range.fallback_minutes, 30);

        wait_for_config_mtime_tick();
        write_hot_reload_config(&path, "/复盘", 90);
        assert!(reloader.reload_if_changed().unwrap());
        assert_eq!(reloader.config().time_range.fallback_minutes, 90);

        let incoming = incoming_text("/复盘 1h");
        assert!(reloader.matcher().match_message(&incoming).is_some());
        let old_trigger = incoming_text("/总结 1h");
        assert!(reloader.matcher().match_message(&old_trigger).is_none());

        cleanup_config_path(&path);
    }

    #[test]
    fn config_reloader_keeps_old_config_until_invalid_file_is_fixed() {
        let path = unique_config_path();
        write_hot_reload_config(&path, "/总结", 30);
        let mut reloader = ConfigReloader::load(path.to_str().unwrap()).unwrap();

        wait_for_config_mtime_tick();
        std::fs::write(&path, "not-valid = [").unwrap();
        assert!(!reloader.reload_if_changed().unwrap());
        assert_eq!(reloader.config().time_range.fallback_minutes, 30);

        wait_for_config_mtime_tick();
        write_hot_reload_config(&path, "/总结", 120);
        assert!(reloader.reload_if_changed().unwrap());
        assert_eq!(reloader.config().time_range.fallback_minutes, 120);

        cleanup_config_path(&path);
    }

    #[test]
    fn image_caption_auth_errors_are_detected() {
        assert!(is_image_caption_auth_error(
            r#"invalid response: image caption API returned 401 Unauthorized: {"code":"INVALID_PLATFORM_KEY"}"#
        ));
        assert!(is_image_caption_auth_error(
            "missing or invalid platform key"
        ));
        assert!(!is_image_caption_auth_error(
            "remote image download returned 404"
        ));
    }

    #[test]
    fn recent_observed_messages_counts_only_real_user_text_in_range() {
        let room = "paper2galgame用户群2";
        let base = Utc.with_ymd_and_hms(2026, 6, 2, 14, 30, 0).unwrap();
        let incoming = incoming_message(room, "wxid_self", "/总结", base + Duration::seconds(60));
        let mut recent = RecentObservedMessages::default();

        recent.record(
            &incoming_message(
                room,
                "wxid_user",
                "终于打开了（",
                base + Duration::seconds(10),
            ),
            base + Duration::seconds(10),
        );
        recent.record(
            &incoming_message(
                "other-room",
                "wxid_user",
                "隔壁消息",
                base + Duration::seconds(11),
            ),
            base + Duration::seconds(11),
        );
        recent.record(&incoming, base + Duration::seconds(60));
        recent.record(
            &incoming_message(
                room,
                "wxid_bot",
                "收到 /总结，正在整理文字总结。",
                base + Duration::seconds(61),
            ),
            base + Duration::seconds(61),
        );

        assert_eq!(
            recent.count_user_text_in_range(room, base, base + Duration::seconds(60), &incoming),
            1
        );
    }

    #[test]
    fn recent_observed_messages_prunes_old_events() {
        let room = "paper2galgame用户群2";
        let now = Utc.with_ymd_and_hms(2026, 6, 2, 14, 30, 0).unwrap();
        let mut recent = RecentObservedMessages::default();
        let incoming = incoming_message(room, "wxid_self", "/总结", now);

        recent.record(
            &incoming_message(
                room,
                "wxid_old",
                "很久之前的消息",
                now - Duration::hours(RECENT_OBSERVED_WINDOW_HOURS + 1),
            ),
            now,
        );
        recent.record(
            &incoming_message(room, "wxid_user", "最近的消息", now - Duration::minutes(1)),
            now,
        );

        assert_eq!(
            recent.count_user_text_in_range(room, now - Duration::hours(12), now, &incoming),
            1
        );
    }

    fn incoming_message(
        room_id: &str,
        sender_id: &str,
        content: &str,
        timestamp: DateTime<Utc>,
    ) -> IncomingMessage {
        IncomingMessage {
            room_id: room_id.to_string(),
            room_name: Some(room_id.to_string()),
            sender_id: sender_id.to_string(),
            sender_name: None,
            content: content.to_string(),
            msg_type: "text".to_string(),
            timestamp,
            is_self: false,
        }
    }

    #[test]
    fn long_chat_splitter_keeps_whole_messages_and_order() {
        let privacy = PrivacyFilter::new(wechat_summary_core::config::PrivacyConfig::default());
        let messages = (0..8)
            .map(|index| {
                chat_message(
                    1_716_464_700 + index,
                    "alice",
                    &format!("message-{index} {}", "x".repeat(48)),
                )
            })
            .collect::<Vec<_>>();

        let chunks = build_llm_chunk_requests(&messages, &privacy, "{chat_input}", 360);

        assert!(chunks.len() > 1);
        for chunk in &chunks {
            assert!(chunk.prompt_chars <= 360 || chunk.message_count == 1);
        }
        let joined_prompts = chunks
            .iter()
            .map(|chunk| chunk.prompt.as_str())
            .collect::<Vec<_>>()
            .join("\n");
        for index in 0..8 {
            assert!(joined_prompts.contains(&format!("message-{index}")));
        }
        assert!(
            joined_prompts.find("message-0").unwrap() < joined_prompts.find("message-7").unwrap()
        );
    }

    #[test]
    fn long_chat_splitter_keeps_oversized_single_message_intact() {
        let privacy = PrivacyFilter::new(wechat_summary_core::config::PrivacyConfig::default());
        let oversized = "single-oversized ".to_string() + &"x".repeat(1_000);
        let messages = vec![chat_message(1_716_464_700, "alice", &oversized)];

        let chunks = build_llm_chunk_requests(&messages, &privacy, "{chat_input}", 120);

        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].message_count, 1);
        assert!(chunks[0].prompt.contains(&oversized));
    }

    #[test]
    fn chunk_summary_concat_keeps_chunk_order() {
        let combined = format_chunk_summaries_for_final(&[
            ChunkSummary {
                index: 0,
                message_count: 2,
                output: "第一段".into(),
            },
            ChunkSummary {
                index: 1,
                message_count: 3,
                output: "第二段".into(),
            },
        ]);

        assert!(combined.contains("[CHUNK_SUMMARIES]"));
        assert!(combined.find("第一段").unwrap() < combined.find("第二段").unwrap());
        assert!(combined.contains("分段 1/2"));
        assert!(combined.contains("分段 2/2"));
    }

    fn chat_message(ts: i64, sender: &str, content: &str) -> ChatMessage {
        ChatMessage {
            timestamp: Utc.timestamp_opt(ts, 0).unwrap(),
            sender_id: sender.into(),
            sender_name: Some(sender.into()),
            content: content.into(),
            msg_type: "text".into(),
        }
    }

    #[test]
    fn parses_command_time_range_after_trigger_symbol() {
        let trigger = TriggerMatch {
            room_id: "room".into(),
            trigger_symbol: "/总结".into(),
            trigger_content: "/总结 1h".into(),
        };

        assert_eq!(
            parse_summary_command(&trigger, PlatformKindConfig::Wx4py),
            SummaryCommand {
                target_platform: PlatformKindConfig::Wx4py,
                range_minutes: Some(60),
                image_token_present: false,
            }
        );
    }

    #[test]
    fn ignores_non_range_text_after_trigger_symbol() {
        let trigger = TriggerMatch {
            room_id: "room".into(),
            trigger_symbol: "/总结".into(),
            trigger_content: "/总结 刚才说了什么".into(),
        };

        assert_eq!(
            parse_summary_command(&trigger, PlatformKindConfig::Wx4py),
            SummaryCommand {
                target_platform: PlatformKindConfig::Wx4py,
                range_minutes: None,
                image_token_present: false,
            }
        );
    }

    #[test]
    fn summary_command_defaults_to_source_platform() {
        let command = parse_summary_command_args("2h", PlatformKindConfig::Discord);

        assert_eq!(
            command,
            SummaryCommand {
                target_platform: PlatformKindConfig::Discord,
                range_minutes: Some(120),
                image_token_present: false,
            }
        );
    }

    #[test]
    fn summary_command_accepts_explicit_platform_and_time() {
        let command = parse_summary_command_args("微信 1d", PlatformKindConfig::Discord);

        assert_eq!(
            command,
            SummaryCommand {
                target_platform: PlatformKindConfig::Wx4py,
                range_minutes: Some(24 * 60),
                image_token_present: false,
            }
        );
    }

    #[test]
    fn summary_command_platform_aliases_are_case_insensitive() {
        for value in ["wx", "WX", "微信", "wechat", "WeChat"] {
            let command = parse_summary_command_args(value, PlatformKindConfig::Discord);
            assert_eq!(command.target_platform, PlatformKindConfig::Wx4py);
        }

        for value in ["dc", "DC", "discord", "Discord"] {
            let command = parse_summary_command_args(value, PlatformKindConfig::Wx4py);
            assert_eq!(command.target_platform, PlatformKindConfig::Discord);
        }
    }

    #[test]
    fn summary_command_accepts_discord_alias_without_time() {
        let command = parse_summary_command_args("dc", PlatformKindConfig::Wx4py);

        assert_eq!(
            command,
            SummaryCommand {
                target_platform: PlatformKindConfig::Discord,
                range_minutes: None,
                image_token_present: false,
            }
        );
    }

    #[test]
    fn summary_command_accepts_image_aliases() {
        for value in ["图片", "image", "IMAGE", "img", "IMG"] {
            let command = parse_summary_command_args(value, PlatformKindConfig::Wx4py);
            assert_eq!(
                command,
                SummaryCommand {
                    target_platform: PlatformKindConfig::Wx4py,
                    range_minutes: None,
                    image_token_present: true,
                }
            );
        }
    }

    #[test]
    fn summary_command_accepts_platform_time_and_image() {
        let command = parse_summary_command_args("wechat 1d img", PlatformKindConfig::Discord);

        assert_eq!(
            command,
            SummaryCommand {
                target_platform: PlatformKindConfig::Wx4py,
                range_minutes: Some(24 * 60),
                image_token_present: true,
            }
        );
    }

    #[test]
    fn summary_command_accepts_image_before_time() {
        let command = parse_summary_command_args("图片 1h", PlatformKindConfig::Discord);

        assert_eq!(
            command,
            SummaryCommand {
                target_platform: PlatformKindConfig::Discord,
                range_minutes: Some(60),
                image_token_present: true,
            }
        );
    }

    #[test]
    fn retry_notice_mentions_attempt_wait_and_operation() {
        let notice = AiRetryNotice {
            operation: "LLM chat completion",
            attempt: 2,
            max_attempts: 6,
            retry_after_ms: 2_000,
            reason: "503 Service Unavailable".into(),
        };

        let message = format_retry_notice(&notice);

        assert!(message.contains("模型请求"));
        assert!(message.contains("第 2/5 次"));
        assert!(message.contains("约 2秒 后"));
        assert!(message.contains("503 Service Unavailable"));
        assert!(is_agent_status_content(&message));
    }

    #[test]
    fn retry_notice_redacts_secret_like_reason() {
        let notice = AiRetryNotice {
            operation: "image generation request",
            attempt: 1,
            max_attempts: 6,
            retry_after_ms: 1_000,
            reason: "upstream rejected sk-test-direct-value-1234567890".into(),
        };

        let message = format_retry_notice(&notice);

        assert!(!message.contains("sk-test"));
        assert!(message.contains("<redacted-secret>"));
    }

    #[test]
    fn recent_trigger_attempts_reject_same_trigger_inside_short_window() {
        let mut attempts = RecentTriggerAttempts::default();
        let trigger = TriggerMatch {
            room_id: "room-a".into(),
            trigger_symbol: "/总结".into(),
            trigger_content: "/总结".into(),
        };
        let now = Utc.with_ymd_and_hms(2026, 5, 29, 12, 25, 38).unwrap();

        assert!(!attempts.is_duplicate(&trigger, now));
        assert!(attempts.is_duplicate(
            &trigger,
            now + Duration::seconds(TRIGGER_DEDUPE_WINDOW_SECONDS - 1)
        ));
        assert!(!attempts.is_duplicate(
            &trigger,
            now + Duration::seconds(TRIGGER_DEDUPE_WINDOW_SECONDS + 1)
        ));
    }

    #[test]
    fn recent_trigger_attempts_allows_different_room_or_content() {
        let mut attempts = RecentTriggerAttempts::default();
        let trigger = TriggerMatch {
            room_id: "room-a".into(),
            trigger_symbol: "/总结".into(),
            trigger_content: "/总结".into(),
        };
        let different_room = TriggerMatch {
            room_id: "room-b".into(),
            ..trigger.clone()
        };
        let different_content = TriggerMatch {
            trigger_content: "/总结 1h".into(),
            ..trigger.clone()
        };
        let now = Utc.with_ymd_and_hms(2026, 5, 29, 12, 25, 38).unwrap();

        assert!(!attempts.is_duplicate(&trigger, now));
        assert!(!attempts.is_duplicate(&different_room, now + Duration::seconds(1)));
        assert!(!attempts.is_duplicate(&different_content, now + Duration::seconds(1)));
    }

    #[test]
    fn rate_limit_uses_last_successful_trigger_time() {
        let mut config = test_config();
        config.rate_limit.successful_request_cooldown_seconds = 300;
        let last_success = Utc.timestamp_opt(1_716_464_700, 0).unwrap();
        let now = last_success + Duration::seconds(120);

        let remaining = rate_limit_remaining(now, Some(last_success), &config).unwrap();

        assert_eq!(remaining.num_seconds(), 180);
    }

    #[test]
    fn rate_limit_allows_after_cooldown() {
        let mut config = test_config();
        config.rate_limit.successful_request_cooldown_seconds = 300;
        let last_success = Utc.timestamp_opt(1_716_464_700, 0).unwrap();
        let now = last_success + Duration::seconds(300);

        assert!(rate_limit_remaining(now, Some(last_success), &config).is_none());
    }

    #[test]
    fn image_cooldown_starts_after_summary_cooldown() {
        let mut config = test_config();
        config.rate_limit.successful_request_cooldown_seconds = 300;
        config.rate_limit.successful_image_cooldown_seconds = 600;
        let last_image_success = Utc.timestamp_opt(1_716_464_700, 0).unwrap();

        let remaining = image_cooldown_remaining(
            last_image_success + Duration::seconds(301),
            Some(last_image_success),
            &config,
        )
        .unwrap();

        assert_eq!(remaining.num_seconds(), 599);
        assert!(image_cooldown_remaining(
            last_image_success + Duration::seconds(900),
            Some(last_image_success),
            &config
        )
        .is_none());
    }

    #[test]
    fn image_cooldown_is_disabled_when_extra_window_is_zero() {
        let mut config = test_config();
        config.rate_limit.successful_request_cooldown_seconds = 300;
        config.rate_limit.successful_image_cooldown_seconds = 0;
        let last_image_success = Utc.timestamp_opt(1_716_464_700, 0).unwrap();

        assert!(image_cooldown_remaining(
            last_image_success + Duration::seconds(1),
            Some(last_image_success),
            &config
        )
        .is_none());
    }

    #[test]
    fn scheduled_run_defaults_to_next_22_00_beijing() {
        let config = test_config();
        let now = Utc.with_ymd_and_hms(2026, 5, 25, 13, 30, 0).unwrap();
        let run_at = next_scheduled_run_after(now, &config).unwrap();

        assert_eq!(run_at, Utc.with_ymd_and_hms(2026, 5, 25, 14, 0, 0).unwrap());
    }

    #[test]
    fn scheduled_run_rolls_to_tomorrow_after_local_time_passes() {
        let config = test_config();
        let now = Utc.with_ymd_and_hms(2026, 5, 25, 14, 1, 0).unwrap();
        let run_at = next_scheduled_run_after(now, &config).unwrap();

        assert_eq!(run_at, Utc.with_ymd_and_hms(2026, 5, 26, 14, 0, 0).unwrap());
    }

    #[test]
    fn scheduled_run_can_fire_at_exact_local_time() {
        let config = test_config();
        let now = Utc.with_ymd_and_hms(2026, 5, 25, 14, 0, 0).unwrap();
        let run_at = next_scheduled_run_after(now, &config).unwrap();

        assert_eq!(run_at, now);
    }

    #[test]
    fn scheduled_rooms_default_to_wx4py_groups() {
        let config = test_config();

        assert_eq!(
            scheduled_rooms(&config, &["测试群".to_string()]),
            vec!["测试群".to_string()]
        );
    }

    #[test]
    fn scheduled_rooms_config_overrides_platform_rooms() {
        let mut config = test_config();
        config.scheduled_summary.rooms = vec!["定时群".to_string()];

        assert_eq!(
            scheduled_rooms(&config, &["平台群".to_string()]),
            vec!["定时群".to_string()]
        );
    }

    #[test]
    fn manual_pipeline_image_argument_enables_when_default_off() {
        let mut config = test_config();
        config.image_gen.enabled = true;
        config.manual_summary.image_by_default = false;

        assert!(!PipelineOptions::manual(&config, false).image_gen_enabled);
        assert!(PipelineOptions::manual(&config, true).image_gen_enabled);
    }

    #[test]
    fn manual_pipeline_image_argument_disables_when_default_on() {
        let mut config = test_config();
        config.image_gen.enabled = true;
        config.manual_summary.image_by_default = true;

        assert!(PipelineOptions::manual(&config, false).image_gen_enabled);
        assert!(!PipelineOptions::manual(&config, true).image_gen_enabled);
    }

    fn test_config() -> AgentConfig {
        AgentConfig::from_toml_str(
            r#"
            [wx4py]
            groups = ["测试群"]

            [listen]
            triggers = ["/总结"]

            [time_range]

            [rate_limit]

            [storage]
            sqlite_path = ":memory:"

            [llm]
            provider = "openai_compatible"
            api_key_env = "LLM_API_KEY"

            [image_gen]
            enabled = false
            provider = "openai"
            api_key_env = "IMAGE_API_KEY"
            size = "2:3"

            [runtime]
            output_dir = ".\\runtime\\test"
            "#,
        )
        .unwrap()
    }

    fn incoming_text(content: &str) -> IncomingMessage {
        IncomingMessage {
            room_id: "测试群".to_string(),
            room_name: None,
            sender_id: "user".to_string(),
            sender_name: None,
            content: content.to_string(),
            msg_type: "text".to_string(),
            timestamp: Utc::now(),
            is_self: false,
        }
    }

    fn write_hot_reload_config(path: &Path, trigger: &str, fallback_minutes: i64) {
        let base_dir = path.parent().unwrap();
        let sqlite_path = base_dir.join("state.sqlite");
        let runtime_dir = base_dir.join("runtime");
        let text = format!(
            r#"
            [wx4py]
            groups = ["测试群"]

            [listen]
            triggers = [{}]

            [time_range]
            fallback_minutes = {}

            [rate_limit]

            [storage]
            sqlite_path = {}

            [llm]
            provider = "openai_compatible"
            api_key_env = "LLM_API_KEY"

            [image_gen]
            enabled = false
            provider = "openai"
            api_key_env = "IMAGE_API_KEY"
            size = "2:3"

            [runtime]
            output_dir = {}
            "#,
            toml_string(trigger),
            fallback_minutes,
            toml_string(&sqlite_path.to_string_lossy()),
            toml_string(&runtime_dir.to_string_lossy())
        );
        std::fs::write(path, text).unwrap();
    }

    fn unique_config_path() -> PathBuf {
        let unique = format!(
            "summary-agent-hot-reload-{}-{}",
            std::process::id(),
            Utc::now().timestamp_nanos_opt().unwrap()
        );
        let dir = std::env::temp_dir().join(unique);
        std::fs::create_dir_all(&dir).unwrap();
        dir.join("agent.toml")
    }

    fn toml_string(value: &str) -> String {
        format!("{value:?}")
    }

    fn wait_for_config_mtime_tick() {
        std::thread::sleep(TestDuration::from_millis(100));
    }

    fn cleanup_config_path(path: &Path) {
        if let Some(parent) = path.parent() {
            let _ = std::fs::remove_dir_all(parent);
        }
    }
}
