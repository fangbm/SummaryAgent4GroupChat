use std::{
    env,
    fs::{self, OpenOptions},
    io::Write,
    time::Duration as StdDuration,
};

use anyhow::{Context, Result};
use chrono::{DateTime, Duration, TimeZone, Utc};
use tracing::{debug, error, info, warn};
use tracing_subscriber::EnvFilter;
use wechat_summary_ai::{OpenAiCompatibleLlm, OpenAiImageClient};
use wechat_summary_core::{
    config::TimeRangeMode,
    models::{ChatMessage, ImageArtifact, IncomingMessage},
    parse_command_time_range_minutes, AgentConfig, ChatFormatter, PrivacyFilter, ResolvedTimeRange,
    TimeRangeCalculator, TriggerMatch, TriggerMatcher,
};
use wechat_summary_storage::SqliteStateStore;
use wx4py_client::{Wx4pyClient, Wx4pyEvent, Wx4pyHistoryMessage};

#[tokio::main]
async fn main() -> Result<()> {
    let config_path = config_path_from_args();
    let config = AgentConfig::from_path(&config_path)
        .with_context(|| format!("loading config {}", config_path))?;

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new(&config.runtime.log_level))
        .init();

    let store = SqliteStateStore::open(&config.storage.sqlite_path)?;
    let matcher = TriggerMatcher::new(config.listen.clone())?;
    let client = Wx4pyClient::start(&config.wx4py, &config.listen, &config.wx_cli)?;

    info!(
        groups = ?config.wx4py.groups,
        "wx4py message receiving enabled"
    );
    append_runtime_log(
        &config,
        &format!("wx4py enabled groups={:?}", config.wx4py.groups),
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
            handle_wx4py_event(&config, &store, &matcher, &client, event).await?;
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

fn incoming_from_wx4py(event: Wx4pyEvent) -> Option<IncomingMessage> {
    let room_id = event.room_id.clone();
    let timestamp = event.timestamp()?;

    Some(IncomingMessage {
        room_id,
        room_name: event.room_name,
        sender_id: event.sender_id.unwrap_or_else(|| "unknown".to_string()),
        sender_name: event.sender_name,
        content: event.content,
        msg_type: "text".to_string(),
        timestamp,
        is_self: false,
    })
}

async fn handle_wx4py_event(
    config: &AgentConfig,
    store: &SqliteStateStore,
    matcher: &TriggerMatcher,
    client: &Wx4pyClient,
    event: Wx4pyEvent,
) -> Result<()> {
    let event_preview = event.content.chars().take(40).collect::<String>();
    info!(
        room_id = ?event.room_id,
        content_len = event.content.chars().count(),
        content_preview = %event_preview,
        "wx4py event received"
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
    let Some(incoming) = incoming_from_wx4py(event) else {
        debug!("wx4py event skipped before trigger matching");
        append_runtime_log(config, "event skipped before trigger matching");
        return Ok(());
    };
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

    let command_range_minutes = command_range_minutes(&trigger);
    let range = TimeRangeCalculator::resolve_with_override(
        incoming.timestamp,
        last_trigger,
        &config.time_range,
        command_range_minutes,
    );

    info!(
        room_id = %trigger.room_id,
        since = %range.since,
        until = %range.until,
        command_range_minutes = ?command_range_minutes,
        "trigger accepted; running summary pipeline"
    );
    append_runtime_log(
        config,
        &format!(
            "trigger accepted room={} since={} until={} command_range_minutes={:?}",
            trigger.room_id, range.since, range.until, command_range_minutes
        ),
    );

    match run_summary_pipeline(
        config,
        client,
        &incoming,
        &trigger,
        &range,
        PipelineOptions::manual(config),
    )
    .await
    {
        Ok(()) => {
            store.set_last_trigger(&trigger.room_id, incoming.timestamp)?;
            info!(room_id = %trigger.room_id, "summary pipeline completed");
            append_runtime_log(
                config,
                &format!("pipeline completed room={}", trigger.room_id),
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

async fn run_scheduled_summaries(
    config: &AgentConfig,
    store: &SqliteStateStore,
    client: &Wx4pyClient,
    now: DateTime<Utc>,
) -> Result<()> {
    let rooms = scheduled_rooms(config);
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
        )
        .await
        {
            Ok(()) => {
                store.set_last_trigger(&room, now)?;
                info!(room_id = %room, "scheduled summary pipeline completed");
                append_runtime_log(
                    config,
                    &format!("scheduled pipeline completed room={}", room),
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

impl PipelineOptions {
    fn manual(config: &AgentConfig) -> Self {
        Self {
            text_summary_enabled: config.text_summary.enabled,
            image_gen_enabled: config.image_gen.enabled,
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
    client: &Wx4pyClient,
    incoming: &IncomingMessage,
    trigger: &TriggerMatch,
    range: &ResolvedTimeRange,
    options: PipelineOptions,
) -> Result<()> {
    if !options.text_summary_enabled && !options.image_gen_enabled {
        if options.send_disabled_message {
            client
                .send_text(&trigger.room_id, "当前配置未开启文字总结或图片生成。")
                .await
                .context("sending disabled pipeline message")?;
        }
        return Ok(());
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
        return Ok(());
    }

    let mut history = client
        .query_text_messages(
            &trigger.room_id,
            incoming.room_name.as_deref(),
            range.since,
            range.until,
            config.privacy.max_messages_to_llm as u32,
        )
        .await
        .context("querying wx-cli decrypted chat history")?;
    info!(
        room_id = %trigger.room_id,
        history_len = history.len(),
        since = %range.since,
        until = %range.until,
        "wx-cli history query completed"
    );
    history.retain(|message| {
        !is_current_trigger_message(message, incoming) && !is_agent_status_message(message)
    });
    info!(
        room_id = %trigger.room_id,
        history_len = history.len(),
        "history after trigger-message filtering"
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
        return Ok(());
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
        }
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
        let image_summary = llm
            .complete(&config.image_summary.system_prompt, &image_summary_request)
            .await
            .context("calling LLM for image summary")?;
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
        let image_prompt = llm
            .complete(&config.image_prompt.system_prompt, &image_prompt_request)
            .await
            .context("calling LLM for image prompt")?;
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
                if let Some(reply) = pending_text_reply.take() {
                    client
                        .send_text(&trigger.room_id, &reply)
                        .await
                        .context("sending deferred summary text")?;
                    info!(room_id = %trigger.room_id, "deferred summary text sent");
                }
                send_summary_image(config, client, &trigger.room_id, &artifact).await?;
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
                if let Some(reply) = pending_text_reply.take() {
                    client
                        .send_text(&trigger.room_id, &reply)
                        .await
                        .context("sending deferred summary text after image failure")?;
                    info!(room_id = %trigger.room_id, "deferred summary text sent after image failure");
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

    if let Some(reply) = pending_text_reply.take() {
        client
            .send_text(&trigger.room_id, &reply)
            .await
            .context("sending deferred summary text without image")?;
        info!(room_id = %trigger.room_id, "deferred summary text sent without image");
    }

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

async fn send_summary_image(
    config: &AgentConfig,
    client: &Wx4pyClient,
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

fn scheduled_rooms(config: &AgentConfig) -> Vec<String> {
    let rooms = if !config.scheduled_summary.rooms.is_empty() {
        &config.scheduled_summary.rooms
    } else if !config.wx4py.groups.is_empty() {
        &config.wx4py.groups
    } else {
        &config.listen.whitelist_rooms
    };

    rooms
        .iter()
        .filter(|room| !room.trim().is_empty())
        .cloned()
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

fn command_range_minutes(trigger: &TriggerMatch) -> Option<i64> {
    let args = trigger
        .trigger_content
        .strip_prefix(&trigger.trigger_symbol)?
        .trim();
    parse_command_time_range_minutes(args)
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

fn is_current_trigger_message(message: &Wx4pyHistoryMessage, incoming: &IncomingMessage) -> bool {
    message.timestamp == incoming.timestamp
        && message.content.trim() == incoming.content.trim()
        && (message.sender_id == incoming.sender_id || message.is_self)
}

fn is_agent_status_message(message: &Wx4pyHistoryMessage) -> bool {
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

fn history_to_chat_message(message: Wx4pyHistoryMessage) -> ChatMessage {
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
        let history = Wx4pyHistoryMessage {
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
        let message = Wx4pyHistoryMessage {
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

        assert_eq!(command_range_minutes(&trigger), Some(60));
    }

    #[test]
    fn ignores_non_range_text_after_trigger_symbol() {
        let trigger = TriggerMatch {
            room_id: "room".into(),
            trigger_symbol: "/总结".into(),
            trigger_content: "/总结 刚才说了什么".into(),
        };

        assert_eq!(command_range_minutes(&trigger), None);
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

        assert_eq!(scheduled_rooms(&config), vec!["测试群".to_string()]);
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
