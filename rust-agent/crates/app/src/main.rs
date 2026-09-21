use std::{
    cmp::Ordering,
    collections::HashSet,
    env, fs,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration as StdDuration, Instant, SystemTime},
};

mod ai_runtime;
mod config_reloader;
mod event_dispatcher;
mod event_handler;
mod history_rules;
mod image_command;
mod llm_chunking;
mod llm_output;
mod llm_service;
mod media_audio;
mod media_rules;
mod media_service;
mod operational_task;
mod outbox;
mod pipeline_delivery;
mod pipeline_input;
mod platform;
mod platform_runtime;
mod report_schedule;
mod runtime_artifacts;
mod runtime_log;
mod scheduled_backlog;
mod scheduled_runner;
mod summary_command;
mod summary_image;
mod summary_input;
mod summary_pipeline;
mod summary_scheduler;
mod trigger_state;
mod wxdb_watcher;

use ai_runtime::*;
use config_reloader::ConfigReloader;
use event_dispatcher::{enqueue_platform_event, PlatformEventSource};
use event_handler::handle_platform_event;
use image_command::parse as parse_image_command;
#[cfg(test)]
use llm_chunking::*;
#[cfg(test)]
use llm_output::{looks_like_text_summary_refusal, sanitize_llm_visible_output};
#[cfg(test)]
use llm_service::*;
use operational_task::OperationalTask;
use platform_runtime::{PlatformConnectionFingerprint, PlatformRuntime, WxdbWatcherFingerprint};
use runtime_log::*;
use scheduled_backlog::{ScheduledSummaryBacklog, ScheduledSummaryRequest};
#[cfg(test)]
use scheduled_runner::requeue_scheduled_request_after_state_read_failure;
use scheduled_runner::{drain_manual_retry_tasks, drain_scheduled_backlog};
#[cfg(test)]
use std::sync::mpsc;
use summary_command::parse as parse_summary_command;
#[cfg(test)]
use summary_command::{parse_args as parse_summary_command_args, SummaryCommand};
use summary_image::ImagePipelineSlotPool;
#[cfg(test)]
use summary_image::ImagePipelineStage;
use summary_pipeline::*;
use summary_scheduler::{ScheduleResult, SummaryTaskScheduler};
use trigger_state::{RecentObservedMessages, RecentTriggerAttempts};
use wxdb_watcher::{WxdbCommandWatcher, WxdbCommandWatcherRecv};

use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Local, TimeZone, Utc};
#[cfg(test)]
use tokio::sync::{mpsc as tokio_mpsc, oneshot};
use tracing::{error, info, warn};
#[cfg(test)]
use wechat_summary_ai::AiError;
use wechat_summary_ai::OpenAiCompatibleLlm;
use wechat_summary_core::{
    config::{ListenConfig, LlmConfig, MatchMode, PlatformKindConfig, TimeRangeMode},
    models::{ChatMessage, ImageArtifact, IncomingMessage},
    AgentConfig, PrivacyFilter, ResolvedTimeRange, TimeRangeCalculator, TriggerMatch,
    TriggerMatcher,
};
use wechat_summary_storage::{NewTask, SqliteStateStore, TaskState};

use crate::platform::{
    PlatformClient, PlatformEvent, PlatformHistoryCursor, PlatformHistoryMessage, PlatformWorker,
};

// wx4py can replay an already observed command after the first pipeline has
// completed, and those replayed events do not always include a stable ID.
// Keep this long enough to cover that delayed delivery without treating a
// normal later command as the same event.
const TRIGGER_DEDUPE_WINDOW_SECONDS: i64 = 2 * 60;
const TRIGGER_DEDUPE_EVENT_WINDOW_SECONDS: i64 = 5;
const TRIGGER_DEDUPE_RETENTION_SECONDS: i64 = 2 * 60 * 60;
const WXDB_RECOVERED_TRIGGER_REALTIME_DEDUPE_SECONDS: i64 = 30 * 60;
const RECENT_OBSERVED_WINDOW_HOURS: i64 = 6;
const RECENT_OBSERVED_MAX_MESSAGES: usize = 5_000;
const WXDB_COMMAND_WATCH_INTERVAL_SECONDS: u64 = 3;
const WXDB_COMMAND_WATCH_LOOKBACK_SECONDS: i64 = 300;
const WXDB_COMMAND_WATCH_LIMIT: usize = 300;
const WXDB_COMMAND_WATCH_SEEN_IDS: usize = 2_048;
const WXDB_COMMAND_WATCH_ERROR_LOG_INTERVAL_SECONDS: i64 = 5 * 60;
const EMPTY_HISTORY_RETRY_DELAYS_MS: &[u64] = &[1_500, 3_000, 5_000];
const SUMMARY_MAX_CONCURRENCY: usize = 4;
const SUMMARY_PENDING_CAPACITY: usize = 64;
const PLATFORM_RECONNECT_DELAY: StdDuration = StdDuration::from_secs(2);
const WXDB_WATCHER_RESTART_DELAY: StdDuration = StdDuration::from_secs(2);
const TEXT_SUMMARY_REFUSAL_RETRY_PROMPT: &str = r#"
如果聊天记录包含成人、擦边、隐私、争议或其他敏感内容，请只做高层次、中性、脱敏总结：
- 可以概括为“围绕服饰/购物/玩梗/生活闲聊等话题展开”，不要复述露骨细节。
- 不输出违法、露骨、隐私或可识别个人的信息。
- 不要拒绝总结，不要输出“无法给出总结/无法提供内容/无法给到相关内容”。
- 只输出适合直接发回群聊的中文文字总结。
"#;
const IMAGE_PIPELINE_REFUSAL_RETRY_PROMPT: &str = r#"
如果聊天记录或上一步摘要包含成人、擦边、隐私、争议或其他敏感内容，请改为高层次、中性、脱敏的图片总结材料：
- 只保留抽象主题、活跃度、时间线、关键词、情绪趋势、话题分布等安全视觉元素。
- 不复述露骨、违法、隐私或可识别个人的信息。
- 不要拒绝，不要输出“无法给出总结/无法提供内容/无法给到相关内容”。
- 输出必须可直接供下一步图片总结或生图使用。
"#;

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
        .with_env_filter(runtime_env_filter(&config.runtime.log_level))
        .with_writer(RuntimeTraceWriter::new(config))
        .with_ansi(false)
        .init();

    append_runtime_log(config, "agent startup started");
    runtime_artifacts::cleanup(config);

    let store = SqliteStateStore::open(&config.storage.sqlite_path)
        .with_context(|| format!("opening state store {}", config.storage.sqlite_path))?;
    store
        .cleanup_operational_data(config.operations.retention_days)
        .context("cleaning expired operational records")?;
    let (resumed_tasks, review_tasks) = store
        .recover_interrupted_tasks()
        .context("recovering interrupted tasks")?;
    let uncertain_deliveries = store
        .recover_interrupted_deliveries()
        .context("recovering interrupted deliveries")?;
    if resumed_tasks > 0 || review_tasks > 0 || uncertain_deliveries > 0 {
        append_runtime_log(
            config,
            &format!(
                "startup recovery resumed_tasks={} review_tasks={} uncertain_deliveries={}",
                resumed_tasks, review_tasks, uncertain_deliveries
            ),
        );
    }
    let mut platform = PlatformRuntime::start(config).await?;
    let recent_trigger_attempts = Arc::new(Mutex::new(RecentTriggerAttempts::default()));
    let recent_observed_messages = Arc::new(Mutex::new(RecentObservedMessages::default()));
    let mut scheduler =
        SummaryTaskScheduler::new(SUMMARY_MAX_CONCURRENCY, SUMMARY_PENDING_CAPACITY);
    let image_pipeline_slots =
        ImagePipelineSlotPool::new(image_pipeline_slot_capacity(config, &platform.rooms));
    let mut next_artifact_cleanup = Instant::now() + StdDuration::from_secs(6 * 60 * 60);

    info!(
        platforms = ?platform.fingerprint.kinds,
        rooms = ?platform.rooms,
        "platform message receiving enabled"
    );
    append_runtime_log(
        config,
        &format!(
            "platform enabled kinds={:?} rooms={:?}",
            platform.fingerprint.kinds, platform.rooms
        ),
    );

    let mut next_scheduled_run = next_scheduled_run_after(Utc::now(), config);
    if let Some(run_at) = next_scheduled_run {
        info!(
            run_at_utc = %run_at,
            run_at_local = %format_local_time(run_at),
            "scheduled summary enabled"
        );
        append_runtime_log(
            config,
            &format!(
                "scheduled summary enabled next_run_utc={} next_run_beijing={}",
                run_at,
                format_local_time(run_at)
            ),
        );
    }

    let mut scheduled_backlog = ScheduledSummaryBacklog::default();
    let mut next_weekly_runs = report_schedule::schedule(Utc::now(), config);
    loop {
        let old_fingerprint = platform.fingerprint.clone();
        let old_watcher_fingerprint = platform.watcher_fingerprint.clone();
        if config_reloader.reload_if_changed()? {
            let config = config_reloader.config();
            image_pipeline_slots
                .set_capacity(image_pipeline_slot_capacity(config, &platform.rooms));
            let new_fingerprint = PlatformConnectionFingerprint::from_config(config);
            if new_fingerprint != old_fingerprint {
                platform.request_reconnect(config, "platform connection configuration changed");
            } else {
                platform.refresh_runtime_options(config)?;
            }
            if WxdbWatcherFingerprint::from_config(config) != old_watcher_fingerprint {
                platform.restart_watcher(config, "configuration changed");
            }
            if !config.scheduled_summary.enabled {
                scheduled_backlog.clear();
            }
            next_scheduled_run = next_scheduled_run_after(Utc::now(), config);
            next_weekly_runs = report_schedule::schedule(Utc::now(), config);
            if let Some(run_at) = next_scheduled_run {
                info!(
                    run_at_utc = %run_at,
                    run_at_local = %format_local_time(run_at),
                    "scheduled summary replanned after config reload"
                );
            }
        }

        let now = Utc::now();
        let config = config_reloader.config();
        platform.reconnect_if_due(config).await;
        platform.restart_watcher_if_due(config);
        scheduler.reap(config);
        if let Err(error) = drain_outbox(config, &store, &platform.worker).await {
            warn!(error = %error, "outbox drain failed");
        }
        if Instant::now() >= next_artifact_cleanup {
            runtime_artifacts::cleanup(config);
            let _ = store.cleanup_operational_data(config.operations.retention_days);
            next_artifact_cleanup = Instant::now() + StdDuration::from_secs(6 * 60 * 60);
        }
        if next_scheduled_run.is_some_and(|run_at| now >= run_at) {
            let rooms = scheduled_rooms(config, &platform.rooms);
            scheduled_backlog.add_rooms(
                rooms,
                ResolvedTimeRange {
                    since: now - Duration::hours(config.scheduled_summary.range_hours.max(1)),
                    until: now,
                    mode: TimeRangeMode::FixedHours,
                },
                now,
            );
            let config = config_reloader.config();
            next_scheduled_run = next_scheduled_run_after(now + Duration::seconds(1), config);
            if let Some(run_at) = next_scheduled_run {
                info!(
                    run_at_utc = %run_at,
                    run_at_local = %format_local_time(run_at),
                    "next scheduled summary planned"
                );
            }
        }

        let due_weekly_reports = next_weekly_runs
            .iter()
            .filter(|(_, run_at)| now >= **run_at)
            .map(|(name, _)| name.clone())
            .collect::<Vec<_>>();
        for name in due_weekly_reports {
            let config_for_report = config.clone();
            let store_for_report = store.clone();
            let worker_for_report = platform.worker.clone();
            let task_group_name = name.clone();
            tokio::spawn(async move {
                if let Err(error) = run_weekly_group_report(
                    &config_for_report,
                    &store_for_report,
                    &worker_for_report,
                    &task_group_name,
                )
                .await
                {
                    append_runtime_log(
                        &config_for_report,
                        &format!(
                            "weekly report failed group={} error={error:#}",
                            task_group_name
                        ),
                    );
                }
            });
            if let Some(group) = config.report_groups.get(&name) {
                if let Some(next) =
                    report_schedule::next_run_after(now + Duration::seconds(1), group)
                {
                    next_weekly_runs.insert(name, next);
                }
            }
        }

        drain_scheduled_backlog(
            config,
            &store,
            &platform.worker,
            &image_pipeline_slots,
            &mut scheduled_backlog,
            &mut scheduler,
        );
        drain_manual_retry_tasks(
            config,
            &store,
            &platform.worker,
            &image_pipeline_slots,
            &mut scheduler,
        );

        loop {
            let event = match platform.watcher.try_recv() {
                WxdbCommandWatcherRecv::Event(event) => event,
                WxdbCommandWatcherRecv::Empty => break,
                WxdbCommandWatcherRecv::Disconnected => {
                    platform.note_watcher_disconnected(config);
                    break;
                }
            };
            let config = config_reloader.config();
            let matcher = config_reloader.matcher();
            enqueue_platform_event(
                config,
                &store,
                matcher,
                &platform
                    .worker_for(PlatformKindConfig::Wx4py)
                    .unwrap_or_else(|| platform.worker.clone()),
                &recent_trigger_attempts,
                &recent_observed_messages,
                &image_pipeline_slots,
                PlatformEventSource::WxdbRecovered,
                event,
                &mut scheduler,
            );
        }

        let event_clients = platform.event_clients();
        let event = tokio::task::spawn_blocking(move || {
            let per_client_timeout =
                StdDuration::from_millis((1_000 / event_clients.len().max(1) as u64).max(1));
            for (kind, event_client) in event_clients {
                let client_guard = event_client
                    .lock()
                    .map_err(|_| (kind, "platform client mutex poisoned".to_string()))?;
                match client_guard.next_event_timeout(per_client_timeout) {
                    Ok(Some(event)) => return Ok((kind, Some(event))),
                    Ok(None) => {}
                    Err(error) => return Err((kind, format_error_chain(&error))),
                }
            }
            Ok((PlatformKindConfig::Wx4py, None))
        })
        .await
        .context("joining platform event wait")?;
        match event {
            Ok((_, Some(event))) => {
                let config = config_reloader.config();
                let matcher = config_reloader.matcher();
                let Some(worker) = platform.worker_for(event.platform) else {
                    warn!(
                        platform = event.platform.as_str(),
                        "received event from disconnected platform"
                    );
                    continue;
                };
                enqueue_platform_event(
                    config,
                    &store,
                    matcher,
                    &worker,
                    &recent_trigger_attempts,
                    &recent_observed_messages,
                    &image_pipeline_slots,
                    PlatformEventSource::Realtime,
                    event,
                    &mut scheduler,
                );
            }
            Ok((_, None)) => {}
            Err((kind, message)) => {
                error!(platform = kind.as_str(), error = %message, "platform event listener failed; scheduling reconnect");
                append_runtime_log(
                    config_reloader.config(),
                    &format!(
                        "platform event listener failed platform={} error={message}; reconnect scheduled",
                        kind.as_str()
                    ),
                );
                platform.request_reconnect_kind(
                    config_reloader.config(),
                    kind,
                    "platform event listener failed",
                );
            }
        }
    }
}

fn paginate_wxdb_history<F>(
    limit: usize,
    last_seen_local_id: Option<i64>,
    mut query_page: F,
) -> Result<Vec<wx4py_client::Wx4pyHistoryMessage>>
where
    F: FnMut(Option<i64>) -> Result<Vec<wx4py_client::Wx4pyHistoryMessage>>,
{
    let mut before_local_id = None;
    let mut messages = Vec::new();
    let mut seen_ids = HashSet::new();
    loop {
        let page = query_page(before_local_id)?;
        if page.is_empty() {
            break;
        }

        let page_len = page.len();
        let oldest_local_id = page.iter().filter_map(|message| message.local_id).min();
        let reached_watermark = last_seen_local_id.is_some_and(|last_seen| {
            page.iter()
                .filter_map(|message| message.local_id)
                .any(|local_id| local_id <= last_seen)
        });
        for message in page {
            if message
                .local_id
                .is_none_or(|local_id| seen_ids.insert(local_id))
            {
                messages.push(message);
            }
        }
        if reached_watermark || oldest_local_id.is_none() {
            break;
        }
        if page_len < limit {
            break;
        }
        if before_local_id == oldest_local_id {
            break;
        }
        before_local_id = oldest_local_id;
    }

    messages.sort_by(|left, right| {
        left.timestamp.cmp(&right.timestamp).then_with(|| {
            left.local_id
                .cmp(&right.local_id)
                .then_with(|| left.content.cmp(&right.content))
        })
    });
    Ok(messages)
}

fn config_modified_time(path: &Path) -> Option<SystemTime> {
    fs::metadata(path)
        .and_then(|metadata| metadata.modified())
        .ok()
}

fn effective_listen_config(config: &AgentConfig) -> ListenConfig {
    let mut listen = config.listen.clone();
    for kind in config.platform.enabled_kinds() {
        match kind {
            PlatformKindConfig::Wx4py => {
                extend_unique_rooms(&mut listen.whitelist_rooms, &config.wx4py.groups)
            }
            PlatformKindConfig::Discord => {
                extend_unique_rooms(&mut listen.whitelist_rooms, &config.discord.channels)
            }
        }
    }
    listen
}

fn extend_unique_rooms(target: &mut Vec<String>, rooms: &[String]) {
    for room in rooms {
        let room = room.trim();
        if room.is_empty() || target.iter().any(|existing| existing.trim() == room) {
            continue;
        }
        target.push(room.to_string());
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

async fn deliver_outboxed_text(
    config: &AgentConfig,
    task: &OperationalTask,
    client: &PlatformWorker,
    room_id: &str,
    text: &str,
) -> Result<()> {
    outbox::deliver_text(config, task, client, room_id, text).await
}

async fn deliver_outboxed_image(
    config: &AgentConfig,
    task: &OperationalTask,
    client: &PlatformWorker,
    room_id: &str,
    artifact: &ImageArtifact,
) -> Result<()> {
    outbox::deliver_image(config, task, client, room_id, artifact).await
}

async fn drain_outbox(
    config: &AgentConfig,
    store: &SqliteStateStore,
    client: &PlatformWorker,
) -> Result<()> {
    outbox::drain(config, store, client).await
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct DailyBudgetState {
    pub(crate) media_decode_limit: Option<usize>,
}

pub(crate) fn daily_budget_state(
    config: &AgentConfig,
    store: &SqliteStateStore,
    room_id: &str,
    now: DateTime<Utc>,
    image_requested: bool,
) -> Result<DailyBudgetState> {
    if !config.budget.enabled {
        return Ok(DailyBudgetState {
            media_decode_limit: None,
        });
    }
    let local = now.with_timezone(&Local);
    let day_start = Local
        .from_local_datetime(
            &local
                .date_naive()
                .and_hms_opt(0, 0, 0)
                .expect("midnight is valid"),
        )
        .earliest()
        .unwrap_or(local)
        .with_timezone(&Utc);
    let usage = store.daily_usage(room_id, day_start)?;
    let policy = config.room_policy(room_id);
    let summary_limit = policy
        .daily_summary_limit
        .unwrap_or(config.budget.daily_summary_limit);
    let image_limit = policy
        .daily_image_limit
        .unwrap_or(config.budget.daily_image_limit);
    let media_limit = policy
        .daily_media_limit
        .unwrap_or(config.budget.daily_media_limit);
    if summary_limit > 0 && usage.tasks >= summary_limit {
        anyhow::bail!("该群今日总结额度已用完（{}/{summary_limit}）", usage.tasks);
    }
    if image_requested && image_limit > 0 && usage.images >= image_limit {
        anyhow::bail!("该群今日图片额度已用完（{}/{image_limit}）", usage.images);
    }
    Ok(DailyBudgetState {
        media_decode_limit: (media_limit > 0)
            .then(|| media_limit.saturating_sub(usage.media) as usize),
    })
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

fn progress_message(options: &PipelineOptions) -> &'static str {
    match (options.text_summary_enabled, options.image_gen_enabled) {
        (true, true) => "收到 /总结，正在整理群聊并生成图片。",
        (true, false) => "收到 /总结，正在整理文字总结。",
        (false, true) => "收到 /总结，正在整理群聊并生成图片。",
        (false, false) => "当前配置未开启文字总结或图片生成。",
    }
}

fn config_revision(config: &AgentConfig) -> String {
    format!(
        "{}:{}:{}:{}",
        config.platform.kind.as_str(),
        config.llm.provider,
        config.llm.model.as_deref().unwrap_or_default(),
        config.runtime.output_dir
    )
}

/// Skip a known-unhealthy primary provider before spending another request
/// timeout. The regular client still retains the remaining fallback chain.
pub(crate) fn routed_llm_config(
    config: &AgentConfig,
    store: Option<&SqliteStateStore>,
) -> (LlmConfig, bool) {
    let primary_key = format!(
        "{}:{}",
        config.llm.provider,
        config.llm.model.as_deref().unwrap_or("default")
    );
    let circuit_open = store
        .and_then(|store| store.provider_health().ok())
        .and_then(|entries| {
            entries
                .into_iter()
                .find(|entry| entry.capability == "llm" && entry.provider_key == primary_key)
        })
        .and_then(|entry| entry.circuit_open_until)
        .is_some_and(|until| until > Utc::now());
    if !circuit_open {
        return (config.llm.clone(), false);
    }

    let Some((index, fallback)) = config
        .llm
        .fallbacks
        .iter()
        .enumerate()
        .find(|(_, fallback)| !fallback.provider.trim().is_empty())
    else {
        return (config.llm.clone(), false);
    };

    let mut routed = config.llm.clone();
    routed.provider = fallback.provider.clone();
    if fallback.api_key.is_some() {
        routed.api_key = fallback.api_key.clone();
        routed.api_keys.clear();
    }
    if !fallback.api_keys.is_empty() {
        routed.api_keys = fallback.api_keys.clone();
        routed.api_key = None;
    }
    if fallback.base_url.is_some() {
        routed.base_url = fallback.base_url.clone();
    }
    if fallback.model.is_some() {
        routed.model = fallback.model.clone();
    }
    if !fallback.request_body_overrides.is_empty() {
        routed.request_body_overrides = fallback.request_body_overrides.clone();
    }
    routed.fallbacks = config.llm.fallbacks[index + 1..].to_vec();
    (routed, true)
}

fn record_primary_llm_health(store: &SqliteStateStore, config: &AgentConfig, error: Option<&str>) {
    let provider_key = format!(
        "{}:{}",
        config.llm.provider,
        config.llm.model.as_deref().unwrap_or("default")
    );
    let existing = store.provider_health().ok().and_then(|entries| {
        entries
            .into_iter()
            .find(|entry| entry.capability == "llm" && entry.provider_key == provider_key)
    });
    // A successful fallback must not accidentally close the primary's circuit.
    if error.is_none()
        && existing
            .as_ref()
            .and_then(|entry| entry.circuit_open_until)
            .is_some_and(|until| until > Utc::now())
    {
        return;
    }
    let failures = if error.is_some() {
        existing
            .as_ref()
            .map(|entry| entry.consecutive_failures.saturating_add(1))
            .unwrap_or(1)
    } else {
        0
    };
    let circuit_open_until = (failures >= 3).then(|| Utc::now() + Duration::minutes(5));
    if let Err(update_error) =
        store.update_provider_health("llm", &provider_key, failures, circuit_open_until, error)
    {
        warn!(error = %update_error, "failed to persist LLM provider health");
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

    let local_now = now.with_timezone(&Local);
    let local_run_time = local_now.date_naive().and_hms_opt(
        config.scheduled_summary.local_hour,
        config.scheduled_summary.local_minute,
        0,
    )?;
    let run_at: DateTime<Utc> = Local
        .from_local_datetime(&local_run_time)
        .earliest()
        .map(|local| local.with_timezone(&Utc))?;
    if run_at >= now {
        Some(run_at)
    } else {
        Some(run_at + Duration::days(1))
    }
}

async fn run_weekly_group_report(
    config: &AgentConfig,
    store: &SqliteStateStore,
    client: &PlatformWorker,
    group_name: &str,
) -> Result<()> {
    let group = config
        .report_groups
        .get(group_name)
        .filter(|group| group.enabled && !group.rooms.is_empty())
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("weekly report group is disabled or empty"))?;
    let since = Utc::now() - Duration::days(7);
    let history = store.tasks(10_000, None, None)?;
    let tasks = history
        .iter()
        .filter(|task| group.rooms.contains(&task.room_id) && task.created_at >= since)
        .collect::<Vec<_>>();
    let weekly = report_schedule::aggregate(tasks.iter().copied());
    let stats = format!(
        "群组：{group_name}\n统计周期：最近 7 天\n成员群数量：{}\n总结任务：{}\n成功：{}\n失败：{}\n处理消息：{}\n处理媒体：{}\n请生成一张清晰、现代、中文可读的群聊运营统计图表。只呈现聚合指标，不呈现聊天正文、用户名、私人信息或敏感内容。",
        group.rooms.len(), weekly.tasks, weekly.succeeded, weekly.failed, weekly.messages, weekly.media
    );
    for room_id in &group.rooms {
        let report_id = store.create_weekly_metric(group_name, room_id)?;
        let task = OperationalTask {
            id: store
                .create_task(NewTask {
                    room_id,
                    source: "weekly_report",
                    since,
                    until: Utc::now(),
                    config_revision: &config_revision(config),
                    retry_of: None,
                })?
                .id,
            store: store.clone(),
        };
        task.set_stage(
            TaskState::Running,
            "weekly_chart",
            Some(&stats),
            None,
            weekly.tasks as u64,
            weekly.media,
        );
        let outcome = async {
            let artifact = summary_image::generate(config, room_id, &stats, None).await?;
            deliver_outboxed_image(config, &task, client, room_id, &artifact).await?;
            let caption = format!(
                "群组周报：近 7 天共 {} 次总结，成功 {} 次，失败 {} 次。",
                weekly.tasks, weekly.succeeded, weekly.failed
            );
            deliver_outboxed_text(config, &task, client, room_id, &caption).await?;
            store.update_weekly_metric(
                &report_id,
                "succeeded",
                Some(&caption),
                Some(&artifact.path),
                None,
            )?;
            task.set_stage(
                TaskState::Succeeded,
                "weekly_chart_delivered",
                Some(&caption),
                None,
                weekly.tasks as u64,
                weekly.media,
            );
            Ok::<(), anyhow::Error>(())
        }
        .await;
        if let Err(error) = outcome {
            let error_message = format_error_chain(&error);
            let caption = format!("群组周报：近 7 天共 {} 次总结，成功 {} 次，失败 {} 次。统计图生成失败，已降级为文字统计。", weekly.tasks, weekly.succeeded, weekly.failed);
            let _ = deliver_outboxed_text(config, &task, client, room_id, &caption).await;
            store.update_weekly_metric(
                &report_id,
                "degraded",
                Some(&caption),
                None,
                Some(&error_message),
            )?;
            task.set_stage(
                TaskState::Succeeded,
                "weekly_text_fallback",
                Some(&caption),
                Some(&error_message),
                weekly.tasks as u64,
                weekly.media,
            );
        }
    }
    Ok(())
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

pub(crate) fn render_prompt_template(
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

fn format_summary_reply(summary: &str, range: &ResolvedTimeRange, total_messages: usize) -> String {
    format!(
        "群聊总结（{} - {}，{} 条）\n\n{}",
        format_local_time(range.since),
        format_local_time(range.until),
        total_messages,
        summary.trim()
    )
}

pub(crate) fn format_local_time(value: DateTime<Utc>) -> String {
    value
        .with_timezone(&Local)
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

fn summary_media_decode_limit(config: &AgentConfig) -> Option<usize> {
    let mut limit = 0usize;
    if config.image_caption.enabled {
        limit = limit.saturating_add(config.image_caption.max_images_per_summary);
    }
    if config.video_caption.enabled {
        limit = limit.saturating_add(config.video_caption.max_videos_per_summary);
    }
    if config.voice_transcription.enabled {
        limit = limit.saturating_add(config.voice_transcription.max_voices_per_summary);
    }
    Some(limit)
}

fn format_media_decode_limit(limit: Option<usize>) -> String {
    limit
        .map(|limit| limit.to_string())
        .unwrap_or_else(|| "unlimited".to_string())
}

pub(crate) fn history_to_chat_message(message: PlatformHistoryMessage) -> ChatMessage {
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
    use chrono::{Local, TimeZone, Timelike, Utc};
    use std::time::Duration as TestDuration;
    use wechat_summary_ai::AiRetryNotice;

    use super::*;

    #[test]
    fn current_trigger_message_matches_self_history_row() {
        let timestamp = Utc.timestamp_opt(1_716_464_700, 0).unwrap();
        let history = PlatformHistoryMessage {
            stable_id: None,
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
            stable_id: None,
            sender_id: "wxid_self".to_string(),
            sender_name: None,
            content: "/总结".to_string(),
            msg_type: "text".to_string(),
            timestamp,
            is_self: false,
        };

        assert!(history_rules::is_current_trigger(&history, &incoming));
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
    fn detects_short_text_summary_refusals() {
        assert!(looks_like_text_summary_refusal(
            "你好，我无法给到相关内容。"
        ));
        assert!(looks_like_text_summary_refusal(
            "抱歉，我不能提供相关内容。"
        ));
        assert!(!looks_like_text_summary_refusal(
            "大家围绕服饰购买、发货时间、游戏和日常玩梗展开聊天，讨论较分散。"
        ));
        assert!(!looks_like_text_summary_refusal(
            "有人提到商家无法提供明确发货时间，随后大家转向讨论物流和海关问题。"
        ));
    }

    #[test]
    fn rejects_refusal_like_image_pipeline_outputs() {
        let config = test_config();
        let error = summary_image::ensure_output_not_refusal(
            &config,
            "测试群",
            "image prompt",
            "你好，我无法给到相关内容。",
        )
        .unwrap_err();

        assert!(format_error_chain(&error).contains("skipped image generation"));
        assert!(summary_image::ensure_output_not_refusal(
            &config,
            "测试群",
            "image prompt",
            "A concise visual prompt for a group chat summary poster.",
        )
        .is_ok());
    }

    #[test]
    fn detects_agent_status_messages() {
        let message = PlatformHistoryMessage {
            stable_id: None,
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

        assert!(history_rules::is_agent_status(&message));
    }

    #[test]
    fn image_caption_source_prefers_decoded_local_path_and_accepts_urls() {
        let mut message = PlatformHistoryMessage {
            stable_id: None,
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
            media_rules::image_source(&message).as_deref(),
            Some(r"D:\Temp\decoded.jpg")
        );

        message.decoded_media_path = None;
        assert_eq!(
            media_rules::image_source(&message).as_deref(),
            Some("https://cdn.example/image.png")
        );

        message.media_path = Some(r"D:\Temp\raw.dat".into());
        assert_eq!(media_rules::image_source(&message), None);
    }

    #[test]
    fn media_decode_limit_follows_image_caption_config() {
        let mut config = test_config();
        config.image_caption.enabled = false;
        config.image_caption.max_images_per_summary = 20;
        config.voice_transcription.enabled = false;
        config.voice_transcription.max_voices_per_summary = 20;
        assert_eq!(summary_media_decode_limit(&config), Some(0));

        config.image_caption.enabled = true;
        config.image_caption.max_images_per_summary = 7;
        assert_eq!(summary_media_decode_limit(&config), Some(7));
        config.voice_transcription.enabled = true;
        config.voice_transcription.max_voices_per_summary = 3;
        assert_eq!(summary_media_decode_limit(&config), Some(10));
        assert_eq!(format_media_decode_limit(Some(7)), "7");
        assert_eq!(format_media_decode_limit(None), "unlimited");
    }

    #[test]
    fn llm_visible_output_sanitizer_strips_thinking_blocks_and_preludes() {
        let output = "<think>hidden chain of thought</think>\n用户现在需要总结这个超长的群聊记录，首先得按时间线来。第一个，5月24日下午大家讨论AI工具。";
        let sanitized = sanitize_llm_visible_output(output);

        assert!(!sanitized.contains("hidden chain of thought"));
        assert!(!sanitized.contains("用户现在需要总结"));
        assert_eq!(sanitized, "第一个，5月24日下午大家讨论AI工具。");
    }

    #[test]
    fn llm_visible_output_sanitizer_strips_internal_chunk_header() {
        let output = "[CHUNK_SUMMARIES]\n以下是同一段群聊按时间顺序切分后的分段总结。\n\n===== 分段 1/2，10 条 =====\n第一段总结";
        let sanitized = sanitize_llm_visible_output(output);

        assert!(!sanitized.contains("[CHUNK_SUMMARIES]"));
        assert!(sanitized.starts_with("===== 分段 1/2"));
    }

    #[test]
    fn voice_transcode_copies_already_mp3_to_cache() {
        let path = unique_config_path();
        let dir = path.parent().unwrap();
        let source = dir.join("voice.aud");
        std::fs::write(&source, b"ID3 fake mp3 bytes").unwrap();
        let audio_prep = media_audio::VoiceTranscriptionAudioPrep {
            transcode_to_mp3: true,
            ffmpeg_executable: "missing-ffmpeg-for-test".into(),
            mp3_bitrate: "64k".into(),
            cache_dir: dir.join("voice-mp3-cache"),
        };

        let output = media_audio::transcode_voice_source_to_mp3(&audio_prep, &source).unwrap();
        let output_path = PathBuf::from(output);
        assert_eq!(
            output_path.extension().and_then(|value| value.to_str()),
            Some("mp3")
        );
        assert_eq!(std::fs::read(output_path).unwrap(), b"ID3 fake mp3 bytes");

        cleanup_config_path(&path);
    }

    #[test]
    fn voice_transcode_hints_raw_silk_and_amr_inputs() {
        let path = unique_config_path();
        let dir = path.parent().unwrap();
        let silk = dir.join("voice-silk.aud");
        let amr = dir.join("voice-amr.aud");
        std::fs::write(&silk, b"#!SILK_V3 fake").unwrap();
        std::fs::write(&amr, b"#!AMR\nfake").unwrap();

        assert_eq!(media_audio::audio_input_format_hint(&silk), Some("silk"));
        assert_eq!(media_audio::audio_input_format_hint(&amr), Some("amr"));

        cleanup_config_path(&path);
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
        assert!(media_rules::is_auth_error(
            r#"invalid response: image caption API returned 401 Unauthorized: {"code":"INVALID_PLATFORM_KEY"}"#
        ));
        assert!(media_rules::is_auth_error(
            "missing or invalid platform key"
        ));
        assert!(!media_rules::is_auth_error(
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

    #[test]
    fn recent_observed_messages_matches_delayed_wxdb_recovered_trigger() {
        let room = "paper2galgame用户群2";
        let base = Utc.with_ymd_and_hms(2026, 6, 26, 3, 43, 7).unwrap();
        let mut recent = RecentObservedMessages::default();
        let incoming = incoming_message(room, "wxid_user", "/总结12h", base);
        recent.record(&incoming, base);
        let trigger = TriggerMatch {
            room_id: room.into(),
            trigger_symbol: "/总结".into(),
            trigger_content: "/总结12h".into(),
        };

        assert!(recent.has_matching_trigger(
            &trigger,
            &incoming,
            base + Duration::minutes(13),
            WXDB_RECOVERED_TRIGGER_REALTIME_DEDUPE_SECONDS
        ));
        assert!(!recent.has_matching_trigger(
            &trigger,
            &incoming,
            base + Duration::minutes(31),
            WXDB_RECOVERED_TRIGGER_REALTIME_DEDUPE_SECONDS
        ));
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
            stable_id: None,
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
    fn context_length_error_detection_matches_provider_payloads() {
        let error = AiError::InvalidResponse(
            "chat completion API returned 400 Bad Request: {\"error\":{\"message\":\"This model's maximum context length is 262144 tokens. Please reduce the length of the messages.\",\"type\":\"context_length_exceeded\"}}"
                .into(),
        );

        assert!(is_context_length_exceeded_error(&error));
    }

    #[test]
    fn split_llm_chunk_request_keeps_message_boundaries_and_order() {
        let privacy_config = wechat_summary_core::config::PrivacyConfig::default();
        let privacy = PrivacyFilter::new(privacy_config.clone());
        let messages = (0..6)
            .map(|index| chat_message(1_716_464_700 + index, "alice", &format!("msg-{index}")))
            .collect::<Vec<_>>();
        let chunk = build_llm_chunk_request(2, messages, &privacy, "{chat_input}");

        let (left, right) =
            split_llm_chunk_request(&chunk, &privacy_config, "{chat_input}").unwrap();

        assert_eq!(left.index, 2);
        assert_eq!(right.index, 2);
        assert_eq!(left.message_count, 3);
        assert_eq!(right.message_count, 3);
        assert!(left.prompt.contains("msg-0"));
        assert!(left.prompt.contains("msg-2"));
        assert!(!left.prompt.contains("msg-3"));
        assert!(right.prompt.contains("msg-3"));
        assert!(right.prompt.contains("msg-5"));
    }

    #[test]
    fn chunk_summary_output_keeps_chunk_order() {
        let combined = format_chunk_summaries_for_output(&[
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
            Some(SummaryCommand {
                target_platform: PlatformKindConfig::Wx4py,
                range_minutes: Some(60),
                image_token_present: false,
                preview_only: false,
                sender_filter: None,
            })
        );
    }

    #[test]
    fn parses_compact_command_time_range_after_trigger_symbol() {
        let trigger = TriggerMatch {
            room_id: "room".into(),
            trigger_symbol: "/总结".into(),
            trigger_content: "/总结1h".into(),
        };

        assert_eq!(
            parse_summary_command(&trigger, PlatformKindConfig::Wx4py),
            Some(SummaryCommand {
                target_platform: PlatformKindConfig::Wx4py,
                range_minutes: Some(60),
                image_token_present: false,
                preview_only: false,
                sender_filter: None,
            })
        );
    }

    #[test]
    fn rejects_non_range_text_after_trigger_symbol() {
        let trigger = TriggerMatch {
            room_id: "room".into(),
            trigger_symbol: "/总结".into(),
            trigger_content: "/总结 刚才说了什么".into(),
        };

        assert!(parse_summary_command(&trigger, PlatformKindConfig::Wx4py).is_none());
    }

    #[test]
    fn rejects_natural_language_suffix_after_trigger_symbol() {
        let trigger = TriggerMatch {
            room_id: "room".into(),
            trigger_symbol: "/总结".into(),
            trigger_content: "/总结并推荐几个能看猫的网站".into(),
        };

        assert!(parse_summary_command(&trigger, PlatformKindConfig::Wx4py).is_none());
    }

    #[test]
    fn rejects_extra_text_after_valid_time_range() {
        let command = parse_summary_command_args("1h extra", PlatformKindConfig::Discord);

        assert!(command.is_none());
    }

    #[test]
    fn summary_command_defaults_to_source_platform() {
        let command = parse_summary_command_args("2h", PlatformKindConfig::Discord).unwrap();

        assert_eq!(
            command,
            SummaryCommand {
                target_platform: PlatformKindConfig::Discord,
                range_minutes: Some(120),
                image_token_present: false,
                preview_only: false,
                sender_filter: None,
            }
        );
    }

    #[test]
    fn summary_command_accepts_explicit_platform_and_time() {
        let command = parse_summary_command_args("微信 1d", PlatformKindConfig::Discord).unwrap();

        assert_eq!(
            command,
            SummaryCommand {
                target_platform: PlatformKindConfig::Wx4py,
                range_minutes: Some(24 * 60),
                image_token_present: false,
                preview_only: false,
                sender_filter: None,
            }
        );
    }

    #[test]
    fn summary_command_platform_aliases_are_case_insensitive() {
        for value in ["wx", "WX", "微信", "wechat", "WeChat"] {
            let command = parse_summary_command_args(value, PlatformKindConfig::Discord).unwrap();
            assert_eq!(command.target_platform, PlatformKindConfig::Wx4py);
        }

        for value in ["dc", "DC", "discord", "Discord"] {
            let command = parse_summary_command_args(value, PlatformKindConfig::Wx4py).unwrap();
            assert_eq!(command.target_platform, PlatformKindConfig::Discord);
        }
    }

    #[test]
    fn summary_command_accepts_discord_alias_without_time() {
        let command = parse_summary_command_args("dc", PlatformKindConfig::Wx4py).unwrap();

        assert_eq!(
            command,
            SummaryCommand {
                target_platform: PlatformKindConfig::Discord,
                range_minutes: None,
                image_token_present: false,
                preview_only: false,
                sender_filter: None,
            }
        );
    }

    #[test]
    fn summary_command_accepts_image_aliases() {
        for value in ["图片", "image", "IMAGE", "img", "IMG"] {
            let command = parse_summary_command_args(value, PlatformKindConfig::Wx4py).unwrap();
            assert_eq!(
                command,
                SummaryCommand {
                    target_platform: PlatformKindConfig::Wx4py,
                    range_minutes: None,
                    image_token_present: true,
                    preview_only: false,
                    sender_filter: None,
                }
            );
        }
    }

    #[test]
    fn summary_command_accepts_platform_time_and_image() {
        let command =
            parse_summary_command_args("wechat 1d img", PlatformKindConfig::Discord).unwrap();

        assert_eq!(
            command,
            SummaryCommand {
                target_platform: PlatformKindConfig::Wx4py,
                range_minutes: Some(24 * 60),
                image_token_present: true,
                preview_only: false,
                sender_filter: None,
            }
        );
    }

    #[test]
    fn summary_command_accepts_image_before_time() {
        let command = parse_summary_command_args("图片 1h", PlatformKindConfig::Discord).unwrap();

        assert_eq!(
            command,
            SummaryCommand {
                target_platform: PlatformKindConfig::Discord,
                range_minutes: Some(60),
                image_token_present: true,
                preview_only: false,
                sender_filter: None,
            }
        );
    }

    #[test]
    fn retry_log_entry_mentions_attempt_wait_and_operation() {
        let notice = AiRetryNotice {
            operation: "LLM chat completion",
            attempt: 2,
            max_attempts: 6,
            retry_after_ms: 2_000,
            reason: "503 Service Unavailable".into(),
        };

        let message = format_retry_log_entry("room-a", &notice);

        assert!(message.contains("room=room-a"));
        assert!(message.contains("operation=LLM chat completion"));
        assert!(message.contains("retry=2/5"));
        assert!(message.contains("wait_ms=2000"));
        assert!(message.contains("503 Service Unavailable"));
    }

    #[test]
    fn retry_log_entry_redacts_secret_like_reason() {
        let notice = AiRetryNotice {
            operation: "image generation request",
            attempt: 1,
            max_attempts: 6,
            retry_after_ms: 1_000,
            reason: "upstream rejected sk-test-direct-value-1234567890".into(),
        };

        let message = format_retry_log_entry("room-a", &notice);

        assert!(!message.contains("sk-test"));
        assert!(message.contains("<redacted-secret>"));
    }

    #[test]
    fn failure_message_for_chat_is_compact_and_redacted() {
        let error = format!(
            "upstream failed sk-test-direct-value-1234567890 {}",
            "x".repeat(900)
        );
        let message = format_failure_message_for_chat("总结失败", &error);

        assert!(message.starts_with("总结失败："));
        assert!(!message.contains("sk-test"));
        assert!(message.contains("<redacted-secret>"));
        assert!(message.contains("完整错误见终端/日志"));
        assert!(message.chars().count() < 800);
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

        assert!(!attempts.is_duplicate_at(&trigger, now, now));
        assert!(attempts.is_duplicate_at(
            &trigger,
            now + Duration::seconds(1),
            now + Duration::seconds(TRIGGER_DEDUPE_WINDOW_SECONDS - 1)
        ));
        assert!(!attempts.is_duplicate_at(
            &trigger,
            now + Duration::seconds(TRIGGER_DEDUPE_EVENT_WINDOW_SECONDS + 1),
            now + Duration::seconds(TRIGGER_DEDUPE_WINDOW_SECONDS + 1)
        ));
    }

    #[test]
    fn recent_trigger_attempts_reject_out_of_order_duplicate_trigger() {
        let mut attempts = RecentTriggerAttempts::default();
        let trigger = TriggerMatch {
            room_id: "room-a".into(),
            trigger_symbol: "/总结".into(),
            trigger_content: "/总结 5h".into(),
        };
        let wx4py_seen_at = Utc.with_ymd_and_hms(2026, 6, 7, 12, 33, 34).unwrap();
        let wxdb_seen_at = wx4py_seen_at - Duration::seconds(1);

        assert!(!attempts.is_duplicate_at(&trigger, wx4py_seen_at, wx4py_seen_at));
        assert!(attempts.is_duplicate_at(
            &trigger,
            wxdb_seen_at,
            wx4py_seen_at + Duration::minutes(30)
        ));
    }

    #[test]
    fn recent_trigger_attempts_reject_delayed_replay_with_same_event_time() {
        let mut attempts = RecentTriggerAttempts::default();
        let trigger = TriggerMatch {
            room_id: "paper2galgame种子用户群".into(),
            trigger_symbol: "/总结".into(),
            trigger_content: "/总结1h".into(),
        };
        let event_at = Utc.with_ymd_and_hms(2026, 6, 17, 12, 28, 12).unwrap();
        let first_observed_at = event_at + Duration::seconds(10);
        let delayed_observed_at = first_observed_at + Duration::minutes(48);

        assert!(!attempts.is_duplicate_at(&trigger, event_at, first_observed_at));
        assert!(attempts.is_duplicate_at(
            &trigger,
            event_at + Duration::seconds(1),
            delayed_observed_at
        ));
    }

    #[test]
    fn recent_trigger_attempts_rejects_missing_id_replay_after_pipeline_completion() {
        let mut attempts = RecentTriggerAttempts::default();
        let trigger = TriggerMatch {
            room_id: "paper2galgame用户群2".into(),
            trigger_symbol: "/总结".into(),
            trigger_content: "/总结 12h".into(),
        };
        let first_event_at = Utc.with_ymd_and_hms(2026, 8, 23, 5, 28, 3).unwrap();
        let first_observed_at = first_event_at + Duration::seconds(3);
        let delayed_observed_at = first_observed_at + Duration::seconds(86);

        assert!(!attempts.is_duplicate_at_with_id(
            &trigger,
            Some("wxdb:12345"),
            first_event_at,
            first_observed_at,
        ));
        assert!(attempts.is_duplicate_at_with_id(
            &trigger,
            None,
            delayed_observed_at,
            delayed_observed_at,
        ));
    }

    #[test]
    fn recent_trigger_attempts_allow_same_command_with_new_event_time() {
        let mut attempts = RecentTriggerAttempts::default();
        let trigger = TriggerMatch {
            room_id: "room-a".into(),
            trigger_symbol: "/总结".into(),
            trigger_content: "/总结1h".into(),
        };
        let first_event_at = Utc.with_ymd_and_hms(2026, 6, 17, 12, 28, 12).unwrap();
        let second_event_at = first_event_at + Duration::minutes(10);

        assert!(!attempts.is_duplicate_at(&trigger, first_event_at, first_event_at));
        assert!(!attempts.is_duplicate_at(
            &trigger,
            second_event_at,
            first_event_at + Duration::minutes(10)
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

        assert!(!attempts.is_duplicate_at(&trigger, now, now));
        assert!(!attempts.is_duplicate_at(
            &different_room,
            now + Duration::seconds(1),
            now + Duration::seconds(1)
        ));
        assert!(!attempts.is_duplicate_at(
            &different_content,
            now + Duration::seconds(1),
            now + Duration::seconds(1)
        ));
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
    fn scheduled_run_defaults_to_next_configured_local_time() {
        let config = test_config();
        let now = Utc.with_ymd_and_hms(2026, 5, 25, 13, 30, 0).unwrap();
        let run_at = next_scheduled_run_after(now, &config).unwrap();

        let expected_today: DateTime<Utc> = Local
            .from_local_datetime(
                &now.with_timezone(&Local)
                    .date_naive()
                    .and_hms_opt(22, 0, 0)
                    .unwrap(),
            )
            .earliest()
            .unwrap()
            .into();
        let expected = if expected_today >= now {
            expected_today
        } else {
            expected_today + Duration::days(1)
        };
        assert_eq!(run_at, expected);
    }

    #[test]
    fn scheduled_run_rolls_to_tomorrow_after_local_time_passes() {
        let config = test_config();
        let now = Utc::now();
        let run_at = next_scheduled_run_after(now, &config).unwrap();

        assert!(run_at > now);
        assert!(run_at - now <= Duration::hours(24));
        let local_run = run_at.with_timezone(&Local);
        assert_eq!(local_run.hour(), 22);
        assert_eq!(local_run.minute(), 0);
    }

    #[test]
    fn scheduled_run_can_fire_at_exact_local_time() {
        let config = test_config();
        let today_run: DateTime<Utc> = Local
            .from_local_datetime(&Local::now().date_naive().and_hms_opt(22, 0, 0).unwrap())
            .earliest()
            .unwrap()
            .into();
        let run_at = next_scheduled_run_after(today_run, &config).unwrap();

        assert_eq!(run_at, today_run);
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
    fn image_pipeline_capacity_tracks_enabled_scheduled_rooms() {
        let mut config =
            AgentConfig::from_toml_str(include_str!("../../../config/agent.toml")).unwrap();
        config.scheduled_summary.rooms = vec!["群A".into(), "群B".into(), "群C".into()];
        config.room_capabilities.insert(
            "群B".into(),
            wechat_summary_core::config::RoomCapabilityConfig {
                image_summary_enabled: Some(false),
                ..Default::default()
            },
        );

        assert_eq!(image_pipeline_slot_capacity(&config, &[]), 2);
        config.image_pipeline.max_concurrent_requests = 1;
        assert_eq!(image_pipeline_slot_capacity(&config, &[]), 1);
        config.image_pipeline.max_concurrent_requests = 8;
        assert_eq!(image_pipeline_slot_capacity(&config, &[]), 2);
    }

    #[tokio::test]
    async fn image_pipeline_slot_pool_prioritizes_prompts() {
        let pool = ImagePipelineSlotPool::new(1);
        let held = pool
            .acquire("already-running", ImagePipelineStage::Summary)
            .await
            .unwrap();
        let (order_sender, mut order_receiver) = tokio_mpsc::unbounded_channel();

        let (summary_ready_sender, summary_ready_receiver) = oneshot::channel();
        let (summary_release_sender, summary_release_receiver) = oneshot::channel();
        let summary_pool = pool.clone();
        let summary_order_sender = order_sender.clone();
        let summary_task = tokio::spawn(async move {
            let _ = summary_ready_sender.send(());
            let lease = summary_pool
                .acquire("queued-summary", ImagePipelineStage::Summary)
                .await
                .unwrap();
            let _ = summary_order_sender.send("summary");
            let _ = summary_release_receiver.await;
            drop(lease);
        });
        summary_ready_receiver.await.unwrap();
        tokio::time::sleep(TestDuration::from_millis(10)).await;

        let (prompt_ready_sender, prompt_ready_receiver) = oneshot::channel();
        let (prompt_release_sender, prompt_release_receiver) = oneshot::channel();
        let prompt_pool = pool.clone();
        let prompt_task = tokio::spawn(async move {
            let _ = prompt_ready_sender.send(());
            let lease = prompt_pool
                .acquire("ready-prompt", ImagePipelineStage::Prompt)
                .await
                .unwrap();
            let _ = order_sender.send("prompt");
            let _ = prompt_release_receiver.await;
            drop(lease);
        });
        prompt_ready_receiver.await.unwrap();
        tokio::time::sleep(TestDuration::from_millis(10)).await;

        drop(held);
        let first = tokio::time::timeout(TestDuration::from_secs(1), order_receiver.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first, "prompt");
        prompt_release_sender.send(()).unwrap();

        let second = tokio::time::timeout(TestDuration::from_secs(1), order_receiver.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(second, "summary");
        summary_release_sender.send(()).unwrap();
        prompt_task.await.unwrap();
        summary_task.await.unwrap();
    }

    #[test]
    fn wxdb_pagination_scans_all_same_second_messages_past_page_limit() {
        let messages = (100..=401)
            .map(|local_id| test_wxdb_history_message(local_id, 1_800_000_000))
            .collect::<Vec<_>>();
        let calls = Arc::new(Mutex::new(Vec::new()));
        let query_messages = messages.clone();
        let query_calls = Arc::clone(&calls);
        let result = paginate_wxdb_history(300, Some(100), move |before_local_id| {
            query_calls.lock().unwrap().push(before_local_id);
            let mut page = query_messages
                .iter()
                .filter(|message| {
                    before_local_id
                        .is_none_or(|before| message.local_id.is_some_and(|id| id < before))
                })
                .rev()
                .take(300)
                .cloned()
                .collect::<Vec<_>>();
            page.sort_by_key(|message| message.local_id);
            Ok(page)
        })
        .unwrap();

        let recovered = result
            .into_iter()
            .filter_map(|message| message.local_id)
            .filter(|local_id| *local_id > 100)
            .collect::<Vec<_>>();
        assert_eq!(recovered.len(), 301);
        assert_eq!(recovered, (101..=401).collect::<Vec<_>>());
        assert_eq!(*calls.lock().unwrap(), vec![None, Some(102)]);
    }

    #[test]
    fn wxdb_watcher_distinguishes_empty_from_disconnected() {
        let (sender, receiver) = mpsc::channel();
        let mut watcher = WxdbCommandWatcher::with_receiver_for_test(receiver);
        assert!(matches!(watcher.try_recv(), WxdbCommandWatcherRecv::Empty));
        drop(sender);
        assert!(matches!(
            watcher.try_recv(),
            WxdbCommandWatcherRecv::Disconnected
        ));
        assert!(matches!(watcher.try_recv(), WxdbCommandWatcherRecv::Empty));
    }

    #[test]
    fn platform_fingerprint_restarts_connections_but_ignores_model_changes() {
        let mut config = test_config();
        let base = PlatformConnectionFingerprint::from_config(&config);

        config.image_gen.enabled = !config.image_gen.enabled;
        assert_eq!(PlatformConnectionFingerprint::from_config(&config), base);

        config.wx4py.command_timeout_seconds += 1;
        assert_ne!(PlatformConnectionFingerprint::from_config(&config), base);
    }

    #[test]
    fn scheduled_backlog_deduplicates_rooms_and_retries_with_backoff() {
        let now = Utc.timestamp_opt(1_800_000_000, 0).unwrap();
        let range = ResolvedTimeRange {
            since: now - Duration::hours(1),
            until: now,
            mode: TimeRangeMode::FixedHours,
        };
        let mut backlog = ScheduledSummaryBacklog::default();
        backlog.add_rooms(
            vec!["room-a".to_string(), "room-b".to_string()],
            range.clone(),
            now,
        );
        backlog.add_rooms(vec!["room-a".to_string()], range, now);
        assert_eq!(backlog.room_ids(), vec!["room-a", "room-b"]);
        backlog.record_retry(Instant::now());
        assert!(!backlog.retry_ready(Instant::now()));
        backlog.clear_retry();
        assert!(backlog.retry_ready(Instant::now()));
    }

    #[test]
    fn scheduled_backlog_requeues_request_after_state_read_failure() {
        let now = Utc.timestamp_opt(1_800_000_000, 0).unwrap();
        let request = ScheduledSummaryRequest {
            room_id: "room-a".to_string(),
            range: ResolvedTimeRange {
                since: now - Duration::hours(1),
                until: now,
                mode: TimeRangeMode::FixedHours,
            },
            due_at: now,
        };
        let expected = request.clone();
        let mut backlog = ScheduledSummaryBacklog::default();
        backlog.requeue_back(request);
        let request = backlog.pop_next().unwrap();

        requeue_scheduled_request_after_state_read_failure(&mut backlog, request);

        assert_eq!(backlog.front(), Some(&expected));
        assert!(backlog.has_retry_scheduled());
        assert!(!backlog.retry_ready(Instant::now()));
    }

    #[test]
    fn wxdb_watcher_fingerprint_tracks_filter_changes() {
        let mut config = test_config();
        let base = WxdbWatcherFingerprint::from_config(&config);

        config.listen.triggers.push("/复盘".to_string());
        assert_ne!(WxdbWatcherFingerprint::from_config(&config), base);

        config = test_config();
        config.listen.match_mode = MatchMode::Contains;
        assert_ne!(WxdbWatcherFingerprint::from_config(&config), base);

        config = test_config();
        config.listen.content_types.push("image".to_string());
        assert_ne!(WxdbWatcherFingerprint::from_config(&config), base);

        config = test_config();
        config.scheduled_summary.range_hours += 1;
        assert_eq!(WxdbWatcherFingerprint::from_config(&config), base);
    }

    #[tokio::test]
    async fn summary_scheduler_enforces_room_and_global_limits() {
        let mut scheduler = SummaryTaskScheduler::new(1, 1);
        let (release_sender, release_receiver) = tokio::sync::oneshot::channel();
        assert_eq!(
            scheduler.enqueue(
                "room-a".to_string(),
                Box::pin(async move {
                    release_receiver
                        .await
                        .map_err(|error| anyhow::anyhow!(error.to_string()))
                }),
            ),
            ScheduleResult::Started
        );
        assert_eq!(
            scheduler.enqueue("room-a".to_string(), Box::pin(async { Ok(()) })),
            ScheduleResult::DuplicateRoom
        );
        assert_eq!(
            scheduler.enqueue("room-b".to_string(), Box::pin(async { Ok(()) })),
            ScheduleResult::Queued
        );
        assert_eq!(
            scheduler.enqueue("room-c".to_string(), Box::pin(async { Ok(()) })),
            ScheduleResult::QueueFull
        );
        release_sender.send(()).unwrap();
        tokio::task::yield_now().await;
        scheduler.reap(&test_config());
        assert_eq!(scheduler.pending_len(), 0);
        assert!(scheduler.is_in_flight("room-b"));
    }

    #[tokio::test]
    async fn summary_scheduler_releases_rooms_after_error_and_panic() {
        let mut scheduler = SummaryTaskScheduler::new(1, 0);
        assert_eq!(
            scheduler.enqueue(
                "room-error".to_string(),
                Box::pin(async { Err(anyhow::anyhow!("expected error")) }),
            ),
            ScheduleResult::Started
        );
        tokio::task::yield_now().await;
        scheduler.reap(&test_config());
        assert!(!scheduler.is_in_flight("room-error"));

        assert_eq!(
            scheduler.enqueue(
                "room-panic".to_string(),
                Box::pin(async { panic!("expected panic") }),
            ),
            ScheduleResult::Started
        );
        tokio::task::yield_now().await;
        scheduler.reap(&test_config());
        assert!(!scheduler.is_in_flight("room-panic"));

        assert_eq!(
            scheduler.enqueue("room-after-failure".to_string(), Box::pin(async { Ok(()) })),
            ScheduleResult::Started
        );
    }

    fn test_wxdb_history_message(
        local_id: i64,
        timestamp: i64,
    ) -> wx4py_client::Wx4pyHistoryMessage {
        wx4py_client::Wx4pyHistoryMessage {
            timestamp: Utc.timestamp_opt(timestamp, 0).unwrap(),
            sender_id: "sender".to_string(),
            sender_name: Some("sender".to_string()),
            content: "/总结".to_string(),
            msg_type: "text".to_string(),
            local_id: Some(local_id),
            media_path: None,
            thumbnail_path: None,
            decoded_media_path: None,
            media_decode_error: None,
            is_self: false,
        }
    }

    #[test]
    fn effective_listen_config_includes_discord_channels() {
        let mut config = test_config();
        config.platform.kind = PlatformKindConfig::Discord;
        config.listen.whitelist_rooms = vec!["微信群".to_string()];
        config.discord.channels = vec!["123456789012345678".to_string()];

        let matcher = TriggerMatcher::new(effective_listen_config(&config)).unwrap();
        let message = IncomingMessage {
            room_id: "123456789012345678".to_string(),
            room_name: Some("general".to_string()),
            stable_id: None,
            sender_id: "user".to_string(),
            sender_name: Some("user".to_string()),
            content: "/总结".to_string(),
            msg_type: "text".to_string(),
            timestamp: Utc::now(),
            is_self: false,
        };

        assert!(matcher.match_message(&message).is_some());

        let mut other_channel = message;
        other_channel.room_id = "234567890123456789".to_string();
        assert!(matcher.match_message(&other_channel).is_none());
    }

    #[test]
    fn effective_listen_config_includes_wx_groups() {
        let mut config = test_config();
        config.listen.whitelist_rooms = vec!["别的群".to_string()];
        config.wx4py.groups = vec!["测试群".to_string()];

        let matcher = TriggerMatcher::new(effective_listen_config(&config)).unwrap();
        let message = IncomingMessage {
            room_id: "测试群".to_string(),
            room_name: Some("测试群".to_string()),
            stable_id: None,
            sender_id: "user".to_string(),
            sender_name: Some("user".to_string()),
            content: "/总结".to_string(),
            msg_type: "text".to_string(),
            timestamp: Utc::now(),
            is_self: false,
        };

        assert!(matcher.match_message(&message).is_some());
    }

    #[test]
    fn manual_pipeline_image_argument_enables_when_default_off() {
        let mut config = test_config();
        config.image_gen.enabled = true;
        config.manual_summary.image_by_default = false;

        assert!(!PipelineOptions::manual(&config, "test-room", false, false).image_gen_enabled);
        assert!(PipelineOptions::manual(&config, "test-room", true, false).image_gen_enabled);
    }

    #[test]
    fn manual_pipeline_image_argument_disables_when_default_on() {
        let mut config = test_config();
        config.image_gen.enabled = true;
        config.manual_summary.image_by_default = true;

        assert!(PipelineOptions::manual(&config, "test-room", false, false).image_gen_enabled);
        assert!(!PipelineOptions::manual(&config, "test-room", true, false).image_gen_enabled);
    }

    #[test]
    fn manual_pipeline_logs_retry_attempts() {
        let config = test_config();

        assert!(PipelineOptions::manual(&config, "test-room", false, false).log_retry_attempts);
    }

    #[test]
    fn scheduled_pipeline_suppresses_retry_attempt_logs() {
        let config = test_config();

        assert!(!PipelineOptions::scheduled(&config, "test-room").log_retry_attempts);
    }

    #[test]
    fn room_capability_disables_manual_and_scheduled_image_summary() {
        let mut config = test_config();
        config.image_gen.enabled = true;
        config.manual_summary.image_by_default = true;
        config.scheduled_summary.send_image = true;
        config.room_capabilities.insert(
            "text-only-room".to_string(),
            wechat_summary_core::config::RoomCapabilityConfig {
                image_summary_enabled: Some(false),
                ..Default::default()
            },
        );

        assert!(
            !PipelineOptions::manual(&config, "text-only-room", false, false).image_gen_enabled
        );
        assert!(PipelineOptions::manual(&config, "other-room", false, false).image_gen_enabled);
        assert!(!PipelineOptions::scheduled(&config, "text-only-room").image_gen_enabled);
        assert!(PipelineOptions::scheduled(&config, "other-room").image_gen_enabled);
    }

    #[test]
    fn runtime_log_limit_rotates_oversized_log() {
        let config_path = unique_config_path();
        let log_path = config_path.parent().unwrap().join("wechat-summary-app.log");
        std::fs::write(&log_path, vec![b'x'; 1024 * 1024 + 16]).unwrap();

        enforce_runtime_log_limit(&log_path, 1);

        let text = std::fs::read_to_string(&log_path).unwrap();
        assert!(text.contains("log rotated because size reached"));
        assert!(std::fs::metadata(&log_path).unwrap().len() < 1024 * 1024);
        let rotated = log_path.with_extension("log.1");
        assert_eq!(std::fs::metadata(&rotated).unwrap().len(), 1024 * 1024 + 16);
    }

    #[test]
    fn runtime_log_limit_zero_disables_rotation() {
        let config_path = unique_config_path();
        let log_path = config_path.parent().unwrap().join("wechat-summary-app.log");
        std::fs::write(&log_path, vec![b'x'; 1024 * 1024 + 16]).unwrap();

        enforce_runtime_log_limit(&log_path, 0);

        assert!(std::fs::metadata(&log_path).unwrap().len() > 1024 * 1024);
        assert!(!log_path.with_extension("log.1").exists());
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

    #[test]
    fn routed_llm_config_skips_an_open_primary_circuit() {
        let mut config = test_config();
        config
            .llm
            .fallbacks
            .push(wechat_summary_core::config::ProviderFallbackConfig {
                provider: "backup-provider".to_string(),
                api_key: Some("backup-key".to_string()),
                api_keys: Vec::new(),
                base_url: Some("https://backup.invalid/v1".to_string()),
                model: Some("backup-model".to_string()),
                request_body_overrides: Default::default(),
            });
        let store = SqliteStateStore::in_memory().unwrap();
        store
            .update_provider_health(
                "llm",
                "openai_compatible:default",
                3,
                Some(Utc::now() + Duration::minutes(5)),
                Some("timeout"),
            )
            .unwrap();

        let (routed, bypassed) = routed_llm_config(&config, Some(&store));
        assert!(bypassed);
        assert_eq!(routed.provider, "backup-provider");
        assert_eq!(routed.model.as_deref(), Some("backup-model"));
        assert_eq!(routed.api_key.as_deref(), Some("backup-key"));
    }

    #[test]
    fn trigger_authorization_only_blocks_when_enabled() {
        let mut config = test_config();
        assert!(config.trigger_user_allowed("测试群", "any-user"));

        config.listen.require_allowed_users = true;
        config.listen.allowed_users = vec!["approved-user".to_string()];
        assert!(config.trigger_user_allowed("测试群", "approved-user"));
        assert!(!config.trigger_user_allowed("测试群", "other-user"));
    }

    fn incoming_text(content: &str) -> IncomingMessage {
        IncomingMessage {
            room_id: "测试群".to_string(),
            room_name: None,
            stable_id: None,
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
