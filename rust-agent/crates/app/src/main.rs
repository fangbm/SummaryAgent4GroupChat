use std::{
    collections::HashMap,
    env,
    fs::{self, OpenOptions},
    io::Write,
    time::Duration as StdDuration,
};

mod platform;

use anyhow::{Context, Result};
use chrono::{DateTime, Duration, TimeZone, Utc};
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;
use wechat_summary_ai::{OpenAiCompatibleLlm, OpenAiImageClient};
use wechat_summary_core::{
    config::{PlatformKindConfig, TimeRangeMode},
    models::{ChatMessage, ImageArtifact, IncomingMessage},
    parse_command_time_range_minutes, AgentConfig, ChatFormatter, PrivacyFilter, ResolvedTimeRange,
    TimeRangeCalculator, TriggerMatch, TriggerMatcher,
};
use wechat_summary_storage::SqliteStateStore;

use crate::platform::{PlatformClient, PlatformEvent, PlatformHistoryMessage, PlatformSender};

const TRIGGER_DEDUPE_WINDOW_SECONDS: i64 = 15;

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
    let config = AgentConfig::from_path(config_path)
        .with_context(|| format!("loading config {}", config_path))?;

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new(&config.runtime.log_level))
        .init();

    append_runtime_log(&config, "agent startup started");
    refresh_wxdb_keys_on_start(&config);

    let store = SqliteStateStore::open(&config.storage.sqlite_path)
        .with_context(|| format!("opening state store {}", config.storage.sqlite_path))?;
    let matcher = TriggerMatcher::new(config.listen.clone()).context("building trigger matcher")?;
    let client = PlatformClient::start(&config)
        .await
        .with_context(|| format!("starting {} platform client", config.platform.kind.as_str()))?;
    let platform_rooms = client.configured_rooms(&config);
    let mut recent_trigger_attempts = RecentTriggerAttempts::default();

    info!(
        platform = client.kind().as_str(),
        rooms = ?platform_rooms,
        "platform message receiving enabled"
    );
    append_runtime_log(
        &config,
        &format!(
            "platform enabled kind={} rooms={:?}",
            client.kind().as_str(),
            platform_rooms
        ),
    );

    let mut next_scheduled_run = next_scheduled_run_after(Utc::now(), &config);
    if let Some(run_at) = next_scheduled_run {
        info!(
            run_at_utc = %run_at,
            run_at_beijing = %format_beijing_time(run_at),
            "scheduled summary enabled"
        );
        append_runtime_log(
            &config,
            &format!(
                "scheduled summary enabled next_run_utc={} next_run_beijing={}",
                run_at,
                format_beijing_time(run_at)
            ),
        );
    }

    loop {
        let now = Utc::now();
        if next_scheduled_run.is_some_and(|run_at| now >= run_at) {
            run_scheduled_summaries(&config, &store, &client, now).await?;
            next_scheduled_run = next_scheduled_run_after(now + Duration::seconds(1), &config);
            if let Some(run_at) = next_scheduled_run {
                info!(
                    run_at_utc = %run_at,
                    run_at_beijing = %format_beijing_time(run_at),
                    "next scheduled summary planned"
                );
            }
        }

        if let Some(event) = client.next_event_timeout(StdDuration::from_secs(1))? {
            handle_platform_event(
                &config,
                &store,
                &matcher,
                &client,
                &mut recent_trigger_attempts,
                event,
            )
            .await?;
        }
    }
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

    let history_message_limit = config.history_message_limit();
    let history_query_limit = history_message_limit.min(u32::MAX as usize) as u32;
    info!(
        room_id = %trigger.room_id,
        since = %range.since,
        until = %range.until,
        limit = history_message_limit,
        "querying platform history"
    );
    append_runtime_log(
        config,
        &format!(
            "history query started room={} since={} until={} limit={}",
            trigger.room_id, range.since, range.until, history_message_limit
        ),
    );
    let mut history = client
        .query_text_messages(
            &trigger.room_id,
            incoming.room_name.as_deref(),
            range.since,
            range.until,
            history_query_limit,
        )
        .await
        .context("querying platform chat history")?;
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
    let llm_input = limit_chars(
        privacy.apply(&formatted.merged_input),
        config.privacy.max_chars_to_llm,
    );
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
    let llm = OpenAiCompatibleLlm::new(config.llm.clone(), &config.proxy)
        .context("initializing LLM client")?;
    let mut pending_text_reply = None;
    if options.text_summary_enabled {
        let text_summary_prompt = render_prompt_template(
            &config.text_summary.user_prompt_template,
            &llm_input,
            "",
            "",
        );
        info!(
            room_id = %trigger.room_id,
            prompt_chars = text_summary_prompt.chars().count(),
            "calling LLM for text summary"
        );
        append_runtime_log(
            config,
            &format!(
                "calling llm text summary room={} prompt_chars={}",
                trigger.room_id,
                text_summary_prompt.chars().count()
            ),
        );
        let summary = llm
            .complete(&config.text_summary.system_prompt, &text_summary_prompt)
            .await
            .context("calling LLM for text summary")?;
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
        let image_summary_request = render_prompt_template(
            &config.image_summary.user_prompt_template,
            &llm_input,
            "",
            "",
        );
        info!(
            room_id = %trigger.room_id,
            prompt_chars = image_summary_request.chars().count(),
            "calling LLM for image summary"
        );
        append_runtime_log(
            config,
            &format!(
                "calling llm image summary room={} prompt_chars={}",
                trigger.room_id,
                image_summary_request.chars().count()
            ),
        );
        let image_summary = match llm
            .complete(&config.image_summary.system_prompt, &image_summary_request)
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
            &llm_input,
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

        match generate_summary_image(config, &trigger.room_id, &image_prompt).await {
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
    text_summary_enabled: bool,
    image_cooldown_recorder: Option<ImageCooldownRecorder>,
) {
    tokio::spawn(async move {
        run_background_image_pipeline(
            config,
            sender,
            room_id,
            llm_input,
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
    text_summary_enabled: bool,
    image_cooldown_recorder: Option<ImageCooldownRecorder>,
) {
    let result = run_background_image_pipeline_inner(
        &config,
        &sender,
        &room_id,
        &llm_input,
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
    image_cooldown_recorder: Option<&ImageCooldownRecorder>,
) -> Result<()> {
    let llm = OpenAiCompatibleLlm::new(config.llm.clone(), &config.proxy)
        .context("initializing LLM client for background image pipeline")?;
    let image_summary_request = render_prompt_template(
        &config.image_summary.user_prompt_template,
        llm_input,
        "",
        "",
    );
    info!(
        room_id = %room_id,
        prompt_chars = image_summary_request.chars().count(),
        "calling LLM for background image summary"
    );
    append_runtime_log(
        config,
        &format!(
            "calling llm background image summary room={} prompt_chars={}",
            room_id,
            image_summary_request.chars().count()
        ),
    );
    let image_summary = llm
        .complete(&config.image_summary.system_prompt, &image_summary_request)
        .await
        .context("calling LLM for background image summary")?;
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

    let image_prompt_request = render_prompt_template(
        &config.image_prompt.user_prompt_template,
        llm_input,
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

    let artifact = generate_summary_image(config, room_id, &image_prompt).await?;
    send_summary_image_with_sender(config, sender, room_id, &artifact).await?;
    record_image_cooldown_success(config, image_cooldown_recorder, room_id)?;
    append_runtime_log(
        config,
        &format!("background image pipeline completed room={}", room_id),
    );
    Ok(())
}

async fn generate_summary_image(
    config: &AgentConfig,
    room_id: &str,
    image_prompt: &str,
) -> Result<ImageArtifact> {
    let image_client = OpenAiImageClient::new(config.image_gen.clone(), &config.proxy)
        .context("initializing image client")?;
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

fn is_agent_status_message(message: &PlatformHistoryMessage) -> bool {
    let content = message.content.trim();
    content.starts_with("收到 /总结")
        || content.starts_with("收到 #总结")
        || content.starts_with("总结失败：")
        || content.starts_with("这段时间没有可总结的文本聊天记录")
        || content.starts_with("当前配置未开启文字总结或图片生成")
        || content.starts_with("群聊总结（")
}

fn limit_chars(input: String, max_chars: usize) -> String {
    if input.chars().count() <= max_chars {
        return input;
    }

    let mut truncated = input.chars().take(max_chars).collect::<String>();
    truncated.push_str("\n\n[内容已按 max_chars_to_llm 截断]");
    truncated
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

    use super::*;

    #[test]
    fn limit_chars_appends_truncation_note() {
        let output = limit_chars("abcdef".to_string(), 3);
        assert!(output.starts_with("abc"));
        assert!(output.contains("截断"));
    }

    #[test]
    fn current_trigger_message_matches_self_history_row() {
        let timestamp = Utc.timestamp_opt(1_716_464_700, 0).unwrap();
        let history = PlatformHistoryMessage {
            timestamp,
            sender_id: "self".to_string(),
            sender_name: Some("我".to_string()),
            content: "/总结".to_string(),
            msg_type: "text".to_string(),
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
            is_self: true,
        };

        assert!(is_agent_status_message(&message));
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
}
