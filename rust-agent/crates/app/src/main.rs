use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet, VecDeque},
    env, fs,
    future::Future,
    path::{Path, PathBuf},
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering as AtomicOrdering},
        mpsc, Arc, Mutex,
    },
    thread,
    time::{Duration as StdDuration, Instant, SystemTime},
};

mod llm_chunking;
mod llm_service;
mod llm_output;
mod summary_image;
mod platform;
mod runtime_log;
mod outbox;
mod report_schedule;
mod summary_input;
mod history_rules;
mod media_rules;
mod media_service;
mod media_audio;
mod ai_runtime;
mod pipeline_delivery;

use runtime_log::*;
use ai_runtime::*;
use summary_image::ImagePipelineSlotPool;
#[cfg(test)]
use llm_chunking::*;
#[cfg(test)]
use llm_service::*;
#[cfg(test)]
use llm_output::{looks_like_text_summary_refusal, sanitize_llm_visible_output};
#[cfg(test)]
use summary_image::ImagePipelineStage;

use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Local, TimeZone, Utc};
use serde::{Deserialize, Serialize};
use tokio::task::JoinSet;
#[cfg(test)]
use tokio::sync::{mpsc as tokio_mpsc, oneshot};
use tracing::{error, info, warn};
use wechat_summary_ai::OpenAiCompatibleLlm;
#[cfg(test)]
use wechat_summary_ai::AiError;
use wechat_summary_core::{
    config::{ListenConfig, MatchMode, PlatformKindConfig, TimeRangeMode},
    models::{ChatMessage, ImageArtifact, IncomingMessage},
    AgentConfig, PrivacyFilter, ResolvedTimeRange, TimeRangeCalculator,
    TriggerMatch, TriggerMatcher,
};
use wechat_summary_storage::{
    NewTask, SqliteStateStore, TaskState,
};

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

type SummaryFuture = Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>>;

struct PendingSummaryTask {
    room_id: String,
    future: SummaryFuture,
}

#[derive(Clone)]
struct OperationalTask {
    pub(crate) id: String,
    pub(crate) store: SqliteStateStore,
}

impl OperationalTask {
    fn set_stage(
        &self,
        state: TaskState,
        stage: &str,
        summary: Option<&str>,
        error: Option<&str>,
        message_count: u64,
        media_count: u64,
    ) {
        if let Err(error) = self.store.update_task(
            &self.id,
            state,
            stage,
            summary,
            error,
            message_count,
            media_count,
        ) {
            warn!(task_id = %self.id, error = %error, "failed to update operational task");
        }
    }

    fn cancelled(&self) -> bool {
        self.store
            .task(&self.id)
            .ok()
            .flatten()
            .is_some_and(|task| task.state == TaskState::Cancelled)
    }
}

struct SummaryTaskScheduler {
    max_concurrency: usize,
    pending_capacity: usize,
    in_flight: HashSet<String>,
    pending: VecDeque<PendingSummaryTask>,
    tasks: JoinSet<()>,
    completion_sender: mpsc::Sender<(String, Result<()>)>,
    completion_receiver: mpsc::Receiver<(String, Result<()>)>,
}

struct SummaryTaskCompletion {
    room_id: Option<String>,
    sender: mpsc::Sender<(String, Result<()>)>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct ScheduledSummaryRequest {
    room_id: String,
    range: ResolvedTimeRange,
    due_at: DateTime<Utc>,
}

#[derive(Default)]
struct ScheduledSummaryBacklog {
    requests: VecDeque<ScheduledSummaryRequest>,
    next_retry_at: Option<Instant>,
    retry_delay: StdDuration,
}

impl ScheduledSummaryBacklog {
    fn add_rooms(
        &mut self,
        rooms: impl IntoIterator<Item = String>,
        range: ResolvedTimeRange,
        due_at: DateTime<Utc>,
    ) {
        for room_id in rooms {
            if !self
                .requests
                .iter()
                .any(|request| request.room_id == room_id)
            {
                self.requests.push_back(ScheduledSummaryRequest {
                    room_id,
                    range: range.clone(),
                    due_at,
                });
            }
        }
    }

    fn is_empty(&self) -> bool {
        self.requests.is_empty()
    }

    fn retry_ready(&self, now: Instant) -> bool {
        self.next_retry_at.is_none_or(|retry_at| now >= retry_at)
    }

    fn record_retry(&mut self, now: Instant) {
        let delay = if self.retry_delay.is_zero() {
            StdDuration::from_secs(1)
        } else {
            self.retry_delay.min(StdDuration::from_secs(30))
        };
        self.next_retry_at = Some(now + delay);
        self.retry_delay = (delay * 2).min(StdDuration::from_secs(30));
    }

    fn clear_retry(&mut self) {
        self.next_retry_at = None;
        self.retry_delay = StdDuration::ZERO;
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct PlatformConnectionFingerprint {
    kind: PlatformKindConfig,
    wx_python: String,
    wx_script: String,
    wx_ready_timeout: u64,
    wx_command_timeout: u64,
    wx_groups: Vec<String>,
    wx_cli_executable: String,
    wx_cli_timeout: u64,
    wx_cli_history_timeout: u64,
    wx_cli_temp_dir: String,
    wx_cli_cache_dir: String,
    wx_cli_db_dir: Option<String>,
    wx_cli_group_name_map: Vec<(String, String)>,
    discord_token: Option<String>,
    discord_token_env: String,
    discord_channels: Vec<String>,
    whitelist_rooms: Vec<String>,
}

impl PlatformConnectionFingerprint {
    fn from_config(config: &AgentConfig) -> Self {
        let mut group_name_map = config
            .wx_cli
            .group_name_map
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<Vec<_>>();
        group_name_map.sort();
        Self {
            kind: config.platform.kind,
            wx_python: config.wx4py.python_executable.clone(),
            wx_script: config.wx4py.sidecar_script.clone(),
            wx_ready_timeout: config.wx4py.ready_timeout_seconds,
            wx_command_timeout: config.wx4py.command_timeout_seconds,
            wx_groups: config.wx4py.groups.clone(),
            wx_cli_executable: config.wx_cli.executable.clone(),
            wx_cli_timeout: config.wx_cli.timeout_seconds,
            wx_cli_history_timeout: config.wx_cli.history_query_timeout_seconds,
            wx_cli_temp_dir: config.wx_cli.temp_dir.clone(),
            wx_cli_cache_dir: config.wx_cli.cache_dir.clone(),
            wx_cli_db_dir: config.wx_cli.db_dir.clone(),
            wx_cli_group_name_map: group_name_map,
            discord_token: config.discord.token.clone(),
            discord_token_env: config.discord.token_env.clone(),
            discord_channels: config.discord.channels.clone(),
            whitelist_rooms: config.listen.whitelist_rooms.clone(),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct WxdbWatcherFingerprint {
    enabled: bool,
    rooms: Vec<String>,
    triggers: Vec<String>,
    match_mode: MatchMode,
    whitelist_rooms: Vec<String>,
    blacklist_users: Vec<String>,
    content_types: Vec<String>,
    ignore_self: bool,
    wx_cli_executable: String,
    wx_cli_timeout: u64,
    wx_cli_history_timeout: u64,
    wx_cli_temp_dir: String,
    wx_cli_cache_dir: String,
    wx_cli_db_dir: Option<String>,
    wx_cli_group_name_map: Vec<(String, String)>,
}

impl WxdbWatcherFingerprint {
    fn from_config(config: &AgentConfig) -> Self {
        let listen = effective_listen_config(config);
        let mut group_name_map = config
            .wx_cli
            .group_name_map
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<Vec<_>>();
        group_name_map.sort();
        Self {
            enabled: wxdb_command_watcher_enabled(config),
            rooms: configured_wx_rooms(config),
            triggers: listen.triggers,
            match_mode: listen.match_mode,
            whitelist_rooms: listen.whitelist_rooms,
            blacklist_users: listen.blacklist_users,
            content_types: listen.content_types,
            ignore_self: listen.ignore_self,
            wx_cli_executable: config.wx_cli.executable.clone(),
            wx_cli_timeout: config.wx_cli.timeout_seconds,
            wx_cli_history_timeout: config.wx_cli.history_query_timeout_seconds,
            wx_cli_temp_dir: config.wx_cli.temp_dir.clone(),
            wx_cli_cache_dir: config.wx_cli.cache_dir.clone(),
            wx_cli_db_dir: config.wx_cli.db_dir.clone(),
            wx_cli_group_name_map: group_name_map,
        }
    }
}

struct PlatformRuntime {
    client: Arc<Mutex<PlatformClient>>,
    worker: PlatformWorker,
    rooms: Vec<String>,
    fingerprint: PlatformConnectionFingerprint,
    watcher: WxdbCommandWatcher,
    watcher_fingerprint: WxdbWatcherFingerprint,
    watcher_restart_at: Option<Instant>,
    reconnect_at: Option<Instant>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum ScheduleResult {
    Started,
    Queued,
    DuplicateRoom,
    QueueFull,
}

impl SummaryTaskCompletion {
    fn finish(mut self, result: Result<()>) {
        if let Some(room_id) = self.room_id.take() {
            let _ = self.sender.send((room_id, result));
        }
    }
}

impl Drop for SummaryTaskCompletion {
    fn drop(&mut self) {
        if let Some(room_id) = self.room_id.take() {
            let _ = self.sender.send((
                room_id,
                Err(anyhow::anyhow!("summary task aborted or panicked")),
            ));
        }
    }
}

impl SummaryTaskScheduler {
    fn new(max_concurrency: usize, pending_capacity: usize) -> Self {
        let (completion_sender, completion_receiver) = mpsc::channel();
        Self {
            max_concurrency: max_concurrency.max(1),
            pending_capacity,
            in_flight: HashSet::new(),
            pending: VecDeque::new(),
            tasks: JoinSet::new(),
            completion_sender,
            completion_receiver,
        }
    }

    fn enqueue(&mut self, room_id: String, future: SummaryFuture) -> ScheduleResult {
        if self.in_flight.contains(&room_id) {
            return ScheduleResult::DuplicateRoom;
        }
        if self.tasks.len() >= self.max_concurrency && self.pending.len() >= self.pending_capacity {
            return ScheduleResult::QueueFull;
        }
        self.in_flight.insert(room_id.clone());
        if self.tasks.len() < self.max_concurrency {
            self.spawn(room_id, future);
            ScheduleResult::Started
        } else {
            self.pending
                .push_back(PendingSummaryTask { room_id, future });
            ScheduleResult::Queued
        }
    }

    fn reap(&mut self, config: &AgentConfig) {
        while let Ok((room_id, result)) = self.completion_receiver.try_recv() {
            self.in_flight.remove(&room_id);
            if let Err(error) = result {
                let error_message = format_error_chain(&error);
                error!(room_id, error = %error_message, "summary task failed");
                append_runtime_log(
                    config,
                    &format!("summary task failed room={room_id} error={error_message}"),
                );
            }
        }
        while let Some(result) = self.tasks.try_join_next() {
            if let Err(error) = result {
                warn!(error = %error, "summary task join failed");
            }
        }
        while self.tasks.len() < self.max_concurrency {
            let Some(task) = self.pending.pop_front() else {
                break;
            };
            self.spawn(task.room_id, task.future);
        }
    }

    fn spawn(&mut self, room_id: String, future: SummaryFuture) {
        let completion = SummaryTaskCompletion {
            room_id: Some(room_id),
            sender: self.completion_sender.clone(),
        };
        self.tasks.spawn(async move {
            let result = future.await;
            completion.finish(result);
        });
    }
}

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
    cleanup_runtime_artifacts(config);

    let store = SqliteStateStore::open(&config.storage.sqlite_path)
        .with_context(|| format!("opening state store {}", config.storage.sqlite_path))?;
    store
        .cleanup_operational_data(config.operations.retention_days)
        .context("cleaning expired operational records")?;
    let mut platform = PlatformRuntime::start(config).await?;
    let recent_trigger_attempts = Arc::new(Mutex::new(RecentTriggerAttempts::default()));
    let recent_observed_messages = Arc::new(Mutex::new(RecentObservedMessages::default()));
    let mut scheduler =
        SummaryTaskScheduler::new(SUMMARY_MAX_CONCURRENCY, SUMMARY_PENDING_CAPACITY);
    let image_pipeline_slots =
        ImagePipelineSlotPool::new(image_pipeline_slot_capacity(config, &platform.rooms));
    let mut next_artifact_cleanup = Instant::now() + StdDuration::from_secs(6 * 60 * 60);

    info!(
        platform = platform.fingerprint.kind.as_str(),
        rooms = ?platform.rooms,
        "platform message receiving enabled"
    );
    append_runtime_log(
        config,
        &format!(
            "platform enabled kind={} rooms={:?}",
            platform.fingerprint.kind.as_str(),
            platform.rooms
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
                platform
                    .client
                    .lock()
                    .map_err(|_| anyhow::anyhow!("platform client mutex poisoned"))?
                    .refresh_runtime_options(config)?;
            }
            if WxdbWatcherFingerprint::from_config(config) != old_watcher_fingerprint {
                platform.restart_watcher(config, "configuration changed");
            }
            if !config.scheduled_summary.enabled {
                scheduled_backlog.requests.clear();
                scheduled_backlog.clear_retry();
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
            cleanup_runtime_artifacts(config);
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
                ).await {
                    append_runtime_log(&config_for_report, &format!("weekly report failed group={} error={error:#}", task_group_name));
                }
            });
            if let Some(group) = config.report_groups.get(&name) {
                if let Some(next) = report_schedule::next_run_after(now + Duration::seconds(1), group) {
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
                &platform.worker,
                &recent_trigger_attempts,
                &recent_observed_messages,
                &image_pipeline_slots,
                PlatformEventSource::WxdbRecovered,
                event,
                &mut scheduler,
            );
        }

        if platform.reconnect_at.is_some() {
            tokio::time::sleep(StdDuration::from_millis(100)).await;
            continue;
        }
        let event_client = Arc::clone(&platform.client);
        let event = tokio::task::spawn_blocking(move || {
            let client_guard = event_client
                .lock()
                .map_err(|_| anyhow::anyhow!("platform client mutex poisoned"))?;
            client_guard.next_event_timeout(StdDuration::from_secs(1))
        })
        .await
        .context("joining platform event wait")?;
        match event {
            Ok(Some(event)) => {
                let config = config_reloader.config();
                let matcher = config_reloader.matcher();
                enqueue_platform_event(
                    config,
                    &store,
                    matcher,
                    &platform.worker,
                    &recent_trigger_attempts,
                    &recent_observed_messages,
                    &image_pipeline_slots,
                    PlatformEventSource::Realtime,
                    event,
                    &mut scheduler,
                );
            }
            Ok(None) => {}
            Err(error) => {
                let message = format_error_chain(&error);
                error!(error = %message, "platform event listener failed; scheduling reconnect");
                append_runtime_log(
                    config_reloader.config(),
                    &format!("platform event listener failed error={message}; reconnect scheduled"),
                );
                platform
                    .request_reconnect(config_reloader.config(), "platform event listener failed");
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn enqueue_platform_event(
    config: &AgentConfig,
    store: &SqliteStateStore,
    matcher: &TriggerMatcher,
    client: &PlatformWorker,
    recent_trigger_attempts: &Arc<Mutex<RecentTriggerAttempts>>,
    recent_observed_messages: &Arc<Mutex<RecentObservedMessages>>,
    image_pipeline_slots: &ImagePipelineSlotPool,
    event_source: PlatformEventSource,
    event: PlatformEvent,
    scheduler: &mut SummaryTaskScheduler,
) {
    let incoming = IncomingMessage::from(event.clone());
    if event_source == PlatformEventSource::Realtime {
        if let Ok(mut recent) = recent_observed_messages.lock() {
            recent.record(&incoming, Utc::now());
        }
    }
    if matcher.match_message(&incoming).is_none() {
        return;
    }

    let room_id = event.room_id.clone();
    let task_config = config.clone();
    let task_store = store.clone();
    let task_client = client.clone();
    let task_attempts = Arc::clone(recent_trigger_attempts);
    let task_observed = Arc::clone(recent_observed_messages);
    let task_image_pipeline_slots = image_pipeline_slots.clone();
    let task_room_id = room_id.clone();
    let future = Box::pin(async move {
        let task_matcher = TriggerMatcher::new(effective_listen_config(&task_config))
            .context("building trigger matcher for summary task")?;
        handle_platform_event(
            &task_config,
            &task_store,
            &task_matcher,
            &task_client,
            &task_attempts,
            &task_observed,
            &task_image_pipeline_slots,
            event_source,
            event,
        )
        .await
    });
    match scheduler.enqueue(room_id, future) {
        ScheduleResult::Started | ScheduleResult::Queued => {}
        ScheduleResult::DuplicateRoom => {
            append_runtime_log(
                config,
                &format!("summary trigger ignored room={task_room_id} reason=in_flight"),
            );
        }
        ScheduleResult::QueueFull => {
            append_runtime_log(
                config,
                &format!("summary trigger rejected room={task_room_id} reason=queue_full"),
            );
        }
    }
}

struct WxdbCommandWatcher {
    receiver: Option<mpsc::Receiver<PlatformEvent>>,
    enabled: bool,
    stop: Option<Arc<AtomicBool>>,
    thread: Option<thread::JoinHandle<()>>,
    state_path: Option<PathBuf>,
}

#[derive(Debug)]
enum WxdbCommandWatcherRecv {
    Event(PlatformEvent),
    Empty,
    Disconnected,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum PlatformEventSource {
    Realtime,
    WxdbRecovered,
}

impl WxdbCommandWatcher {
    fn stopped() -> Self {
        Self {
            receiver: None,
            enabled: false,
            stop: None,
            thread: None,
            state_path: None,
        }
    }

    fn start(config: &AgentConfig) -> Self {
        Self::start_with_state_path(config, None)
    }

    fn start_with_state_path(config: &AgentConfig, previous_state_path: Option<PathBuf>) -> Self {
        if !wxdb_command_watcher_enabled(config) {
            return Self {
                receiver: None,
                enabled: false,
                stop: None,
                thread: None,
                state_path: None,
            };
        }

        let state_path = previous_state_path.unwrap_or_else(|| wxdb_watcher_state_path(config));
        let rooms = configured_wx_rooms(config);
        if rooms.is_empty() {
            append_runtime_log(config, "wxdb command watcher skipped no wx rooms");
            return Self {
                receiver: None,
                enabled: true,
                stop: None,
                thread: None,
                state_path: Some(state_path),
            };
        }

        let config = config.clone();
        let (sender, receiver) = mpsc::channel();
        let stop = Arc::new(AtomicBool::new(false));
        let thread_stop = Arc::clone(&stop);
        let thread_state_path = state_path.clone();
        let thread = thread::spawn(move || {
            run_wxdb_command_watcher(config, rooms, sender, thread_stop, thread_state_path)
        });
        Self {
            receiver: Some(receiver),
            enabled: true,
            stop: Some(stop),
            thread: Some(thread),
            state_path: Some(state_path),
        }
    }

    fn try_recv(&mut self) -> WxdbCommandWatcherRecv {
        let Some(receiver) = self.receiver.as_ref() else {
            return WxdbCommandWatcherRecv::Empty;
        };
        match receiver.try_recv() {
            Ok(event) => WxdbCommandWatcherRecv::Event(event),
            Err(mpsc::TryRecvError::Empty) => WxdbCommandWatcherRecv::Empty,
            Err(mpsc::TryRecvError::Disconnected) => {
                self.receiver = None;
                WxdbCommandWatcherRecv::Disconnected
            }
        }
    }

    fn enabled(&self) -> bool {
        self.enabled
    }

    fn state_path(&self) -> Option<&Path> {
        self.state_path.as_deref()
    }
}

impl Drop for WxdbCommandWatcher {
    fn drop(&mut self) {
        if let Some(stop) = &self.stop {
            stop.store(true, AtomicOrdering::Release);
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn wxdb_command_watcher_enabled(config: &AgentConfig) -> bool {
    config.platform.kind == PlatformKindConfig::Wx4py && !config.wx_cli.executable.trim().is_empty()
}

fn configured_wx_rooms(config: &AgentConfig) -> Vec<String> {
    let rooms = if config.wx4py.groups.is_empty() {
        &config.listen.whitelist_rooms
    } else {
        &config.wx4py.groups
    };
    rooms
        .iter()
        .map(|room| room.trim())
        .filter(|room| !room.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

fn run_wxdb_command_watcher(
    config: AgentConfig,
    rooms: Vec<String>,
    sender: mpsc::Sender<PlatformEvent>,
    stop: Arc<AtomicBool>,
    watermark_path: PathBuf,
) {
    let matcher = match TriggerMatcher::new(effective_listen_config(&config)) {
        Ok(matcher) => matcher,
        Err(error) => {
            append_runtime_log(
                &config,
                &format!("wxdb command watcher failed building matcher error={error}"),
            );
            return;
        }
    };

    append_runtime_log(
        &config,
        &format!(
            "wxdb command watcher started rooms={:?} interval_seconds={} lookback_seconds={} cache_dir={}",
            rooms,
            WXDB_COMMAND_WATCH_INTERVAL_SECONDS,
            WXDB_COMMAND_WATCH_LOOKBACK_SECONDS,
            effective_wxdb_cache_dir(&config)
        ),
    );

    let mut watcher_state = load_wxdb_watcher_state(&watermark_path);
    let startup_now = Utc::now().timestamp();
    for room in &rooms {
        watcher_state
            .rooms
            .entry(room.clone())
            .or_insert_with(|| WxdbWatcherRoomState::new(startup_now));
    }
    save_wxdb_watcher_state(&watermark_path, &watcher_state);
    let mut recent_errors = WxdbCommandWatcherErrors::default();
    loop {
        if stop.load(AtomicOrdering::Acquire) {
            return;
        }
        let now = Utc::now();
        for room in &rooms {
            if stop.load(AtomicOrdering::Acquire) {
                return;
            }
            let state = watcher_state
                .rooms
                .entry(room.clone())
                .or_insert_with(|| WxdbWatcherRoomState::new(now.timestamp()));
            let since_timestamp = state
                .cursor_timestamp
                .saturating_sub(WXDB_COMMAND_WATCH_LOOKBACK_SECONDS);
            let since = Utc
                .timestamp_opt(since_timestamp, 0)
                .single()
                .unwrap_or(now);
            match poll_wxdb_command_room(&config, &matcher, room, since, now, state) {
                Ok(events) => {
                    if stop.load(AtomicOrdering::Acquire) {
                        return;
                    }
                    recent_errors.clear_success(&config, room);
                    for event in events {
                        if stop.load(AtomicOrdering::Acquire) {
                            return;
                        }
                        if sender.send(event).is_err() {
                            append_runtime_log(
                                &config,
                                "wxdb command watcher stopped because receiver closed",
                            );
                            return;
                        }
                    }
                    if stop.load(AtomicOrdering::Acquire) {
                        return;
                    }
                    state.cursor_timestamp = now.timestamp();
                    save_wxdb_watcher_state(&watermark_path, &watcher_state);
                }
                Err(error) => {
                    recent_errors.record(&config, room, &error);
                }
            }
        }
        for _ in 0..WXDB_COMMAND_WATCH_INTERVAL_SECONDS {
            if stop.load(AtomicOrdering::Acquire) {
                return;
            }
            thread::sleep(StdDuration::from_secs(1));
        }
    }
}

fn wxdb_watcher_state_path(config: &AgentConfig) -> PathBuf {
    Path::new(&config.runtime.output_dir).join("wxdb-command-watcher-state.json")
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct WxdbWatcherState {
    rooms: HashMap<String, WxdbWatcherRoomState>,
}

#[derive(Debug, Serialize, Deserialize)]
struct WxdbWatcherRoomState {
    startup_baseline: i64,
    cursor_timestamp: i64,
    #[serde(default)]
    initialized: bool,
    #[serde(default)]
    last_seen_local_id: Option<i64>,
    #[serde(default)]
    seen_ids: VecDeque<String>,
}

impl WxdbWatcherRoomState {
    fn new(now: i64) -> Self {
        Self {
            startup_baseline: now,
            cursor_timestamp: now,
            initialized: false,
            last_seen_local_id: None,
            seen_ids: VecDeque::new(),
        }
    }

    fn contains(&self, id: &str) -> bool {
        self.seen_ids.iter().any(|seen| seen == id)
    }

    fn remember(&mut self, id: String) {
        if self.contains(&id) {
            return;
        }
        self.seen_ids.push_back(id);
        while self.seen_ids.len() > WXDB_COMMAND_WATCH_SEEN_IDS {
            self.seen_ids.pop_front();
        }
    }
}

fn load_wxdb_watcher_state(path: &Path) -> WxdbWatcherState {
    fs::read_to_string(path)
        .ok()
        .and_then(|text| serde_json::from_str(&text).ok())
        .unwrap_or_default()
}

fn save_wxdb_watcher_state(path: &Path, state: &WxdbWatcherState) {
    if let Some(parent) = path.parent() {
        let _ = fs::create_dir_all(parent);
    }
    if let Ok(text) = serde_json::to_string(state) {
        let _ = fs::write(path, text);
    }
}

#[derive(Default)]
struct WxdbCommandWatcherErrors {
    by_room: HashMap<String, WxdbCommandWatcherErrorState>,
}

struct WxdbCommandWatcherErrorState {
    first_seen: DateTime<Utc>,
    last_logged: DateTime<Utc>,
    suppressed: usize,
}

impl WxdbCommandWatcherErrors {
    fn record(&mut self, config: &AgentConfig, room: &str, error: &anyhow::Error) {
        let now = Utc::now();
        let state = self
            .by_room
            .entry(room.to_string())
            .or_insert(WxdbCommandWatcherErrorState {
                first_seen: now,
                last_logged: now - Duration::seconds(WXDB_COMMAND_WATCH_ERROR_LOG_INTERVAL_SECONDS),
                suppressed: 0,
            });

        if now.signed_duration_since(state.last_logged).num_seconds()
            >= WXDB_COMMAND_WATCH_ERROR_LOG_INTERVAL_SECONDS
        {
            append_runtime_log(
                config,
                &format!(
                    "wxdb command watcher poll failed room={} first_seen={} suppressed={} error={}",
                    room,
                    state.first_seen,
                    state.suppressed,
                    compact_error_for_runtime(&format!("{error:#}"), 700)
                ),
            );
            state.last_logged = now;
            state.first_seen = now;
            state.suppressed = 0;
        } else {
            state.suppressed = state.suppressed.saturating_add(1);
        }
    }

    fn clear_success(&mut self, config: &AgentConfig, room: &str) {
        if let Some(state) = self.by_room.remove(room) {
            if state.suppressed > 0 {
                append_runtime_log(
                    config,
                    &format!(
                        "wxdb command watcher poll recovered room={} suppressed_errors={}",
                        room, state.suppressed
                    ),
                );
            }
        }
    }
}

fn poll_wxdb_command_room(
    config: &AgentConfig,
    matcher: &TriggerMatcher,
    room: &str,
    since: DateTime<Utc>,
    until: DateTime<Utc>,
    state: &mut WxdbWatcherRoomState,
) -> Result<Vec<PlatformEvent>> {
    let chat_name = config
        .wx_cli
        .group_name_map
        .get(room)
        .map(String::as_str)
        .unwrap_or(room);
    let query_page = |before_local_id| {
        wx4py_client::query_external_history_page(
            &config.wx_cli,
            chat_name,
            since,
            until,
            WXDB_COMMAND_WATCH_LIMIT as u32,
            Some(0),
            before_local_id,
        )
        .with_context(|| format!("querying wxdb command watcher history for {chat_name}"))
    };

    if !state.initialized {
        let baseline = query_page(None)?;
        state.last_seen_local_id = baseline.iter().filter_map(|message| message.local_id).max();
        state.initialized = true;
        return Ok(Vec::new());
    }

    let messages = paginate_wxdb_history(
        WXDB_COMMAND_WATCH_LIMIT,
        state.last_seen_local_id,
        query_page,
    )?;
    let max_local_id = messages.iter().filter_map(|message| message.local_id).max();
    let mut events = Vec::new();
    for message in messages {
        let Some(key) = wxdb_seen_message_key(chat_name, &message) else {
            tracing::warn!(
                chat_name,
                "wxdb watcher ignored message without stable local_id"
            );
            continue;
        };
        if state.contains(&key)
            || state.last_seen_local_id.is_some_and(|last_seen| {
                message
                    .local_id
                    .is_some_and(|local_id| local_id <= last_seen)
            })
        {
            continue;
        }
        let timestamp = message.timestamp;
        let delayed_new_message = state
            .last_seen_local_id
            .zip(message.local_id)
            .is_some_and(|(last_seen, local_id)| local_id > last_seen);
        if timestamp.timestamp() < state.startup_baseline && !delayed_new_message {
            continue;
        }
        let content = message.content.trim().to_string();
        if content.is_empty() || history_rules::is_agent_status_content(&content) {
            continue;
        }
        let incoming = IncomingMessage {
            room_id: room.to_string(),
            room_name: Some(chat_name.to_string()),
            stable_id: Some(key.clone()),
            sender_id: message.sender_id.clone(),
            sender_name: message.sender_name.clone(),
            content: content.clone(),
            msg_type: "text".to_string(),
            timestamp,
            is_self: message.is_self,
        };
        if matcher.match_message(&incoming).is_none() {
            continue;
        }
        state.remember(key);
        let event = PlatformEvent {
            platform: PlatformKindConfig::Wx4py,
            room_id: incoming.room_id,
            room_name: incoming.room_name,
            stable_id: incoming.stable_id,
            sender_id: incoming.sender_id,
            sender_name: incoming.sender_name,
            content: incoming.content,
            msg_type: incoming.msg_type,
            timestamp: incoming.timestamp,
            is_self: incoming.is_self,
        };
        events.push(event);
    }
    state.last_seen_local_id = max_local_id.or(state.last_seen_local_id);

    events.sort_by(|left, right| {
        left.timestamp.cmp(&right.timestamp).then_with(|| {
            stable_id_number(left.stable_id.as_deref().unwrap_or(""))
                .cmp(&stable_id_number(right.stable_id.as_deref().unwrap_or("")))
        })
    });
    for event in &events {
        append_runtime_log(
            config,
            &format!(
                "wxdb command watcher recovered trigger room={} ts={} content_len={}",
                event.room_id,
                event.timestamp,
                event.content.chars().count()
            ),
        );
    }
    Ok(events)
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

impl PlatformRuntime {
    async fn start(config: &AgentConfig) -> Result<Self> {
        let client = PlatformClient::start(config).await.with_context(|| {
            format!("starting {} platform client", config.platform.kind.as_str())
        })?;
        let worker = client.worker();
        let rooms = client.configured_rooms(config);
        Ok(Self {
            client: Arc::new(Mutex::new(client)),
            worker,
            rooms,
            fingerprint: PlatformConnectionFingerprint::from_config(config),
            watcher: WxdbCommandWatcher::start(config),
            watcher_fingerprint: WxdbWatcherFingerprint::from_config(config),
            watcher_restart_at: None,
            reconnect_at: None,
        })
    }

    fn request_reconnect(&mut self, config: &AgentConfig, reason: &str) {
        if self.reconnect_at.is_none() {
            append_runtime_log(
                config,
                &format!("platform reconnect scheduled reason={reason} delay_seconds=2"),
            );
            self.reconnect_at = Some(Instant::now() + PLATFORM_RECONNECT_DELAY);
        }
    }

    async fn reconnect_if_due(&mut self, config: &AgentConfig) {
        let Some(reconnect_at) = self.reconnect_at else {
            return;
        };
        if Instant::now() < reconnect_at {
            return;
        }

        match PlatformClient::start(config).await {
            Ok(client) => {
                let worker = client.worker();
                let rooms = client.configured_rooms(config);
                let previous_state_path = self.watcher.state_path().map(Path::to_path_buf);
                self.client = Arc::new(Mutex::new(client));
                self.worker = worker;
                self.rooms = rooms;
                self.fingerprint = PlatformConnectionFingerprint::from_config(config);
                let old_watcher =
                    std::mem::replace(&mut self.watcher, WxdbCommandWatcher::stopped());
                drop(old_watcher);
                self.watcher =
                    WxdbCommandWatcher::start_with_state_path(config, previous_state_path);
                self.watcher_fingerprint = WxdbWatcherFingerprint::from_config(config);
                self.watcher_restart_at = None;
                self.reconnect_at = None;
                info!(
                    platform = self.fingerprint.kind.as_str(),
                    "platform reconnected"
                );
                append_runtime_log(
                    config,
                    &format!(
                        "platform reconnected kind={}",
                        self.fingerprint.kind.as_str()
                    ),
                );
            }
            Err(error) => {
                let message = format_error_chain(&error);
                error!(error = %message, "platform reconnect failed; retrying");
                append_runtime_log(
                    config,
                    &format!("platform reconnect failed error={message}; retrying"),
                );
                self.reconnect_at = Some(Instant::now() + PLATFORM_RECONNECT_DELAY);
            }
        }
    }

    fn note_watcher_disconnected(&mut self, config: &AgentConfig) {
        if self.watcher.enabled() && self.watcher_restart_at.is_none() {
            error!("wxdb command watcher channel disconnected; scheduling restart");
            append_runtime_log(
                config,
                "wxdb command watcher channel disconnected; restart scheduled",
            );
            self.watcher_restart_at = Some(Instant::now() + WXDB_WATCHER_RESTART_DELAY);
        }
    }

    fn restart_watcher_if_due(&mut self, config: &AgentConfig) {
        let Some(restart_at) = self.watcher_restart_at else {
            return;
        };
        if Instant::now() >= restart_at {
            let previous_state_path = self.watcher.state_path().map(Path::to_path_buf);
            let old_watcher = std::mem::replace(&mut self.watcher, WxdbCommandWatcher::stopped());
            drop(old_watcher);
            self.watcher = WxdbCommandWatcher::start_with_state_path(config, previous_state_path);
            self.watcher_fingerprint = WxdbWatcherFingerprint::from_config(config);
            self.watcher_restart_at = None;
            append_runtime_log(
                config,
                "wxdb command watcher restarted after channel disconnect",
            );
        }
    }

    fn restart_watcher(&mut self, config: &AgentConfig, reason: &str) {
        let previous_state_path = self.watcher.state_path().map(Path::to_path_buf);
        let state_preserved = previous_state_path.is_some();
        let old_watcher = std::mem::replace(&mut self.watcher, WxdbCommandWatcher::stopped());
        drop(old_watcher);
        self.watcher = WxdbCommandWatcher::start_with_state_path(config, previous_state_path);
        self.watcher_fingerprint = WxdbWatcherFingerprint::from_config(config);
        self.watcher_restart_at = None;
        append_runtime_log(
            config,
            &format!(
                "wxdb command watcher restarted reason={reason} state_preserved={state_preserved}"
            ),
        );
    }
}

fn effective_wxdb_cache_dir(config: &AgentConfig) -> String {
    let cache_dir = config.wx_cli.cache_dir.trim();
    if cache_dir.is_empty() {
        "<default>".to_string()
    } else {
        cache_dir.to_string()
    }
}

fn wxdb_seen_message_key(
    chat_name: &str,
    message: &wx4py_client::Wx4pyHistoryMessage,
) -> Option<String> {
    message
        .local_id
        .map(|local_id| format!("{chat_name}:local:{local_id}"))
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
        let matcher = TriggerMatcher::new(effective_listen_config(&config))
            .context("building trigger matcher")?;
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
        let matcher = match TriggerMatcher::new(effective_listen_config(&config)) {
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
                "config hot reloaded path={} note=platform connection/listener changes trigger controlled reconnect; storage path and runtime log writer remain startup-scoped",
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

fn effective_listen_config(config: &AgentConfig) -> ListenConfig {
    let mut listen = config.listen.clone();
    match config.platform.kind {
        PlatformKindConfig::Wx4py => {
            extend_unique_rooms(&mut listen.whitelist_rooms, &config.wx4py.groups)
        }
        PlatformKindConfig::Discord => {
            extend_unique_rooms(&mut listen.whitelist_rooms, &config.discord.channels)
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

fn cleanup_runtime_artifacts(config: &AgentConfig) {
    const MAX_ARTIFACT_BYTES: u64 = 1024 * 1024 * 1024;
    let cutoff = SystemTime::now()
        .checked_sub(std::time::Duration::from_secs(
            u64::from(config.runtime.cleanup_after_days.max(1)) * 24 * 60 * 60,
        ))
        .unwrap_or(SystemTime::UNIX_EPOCH);
    let files = known_runtime_artifacts(config);
    for (path, _, modified) in &files {
        if *modified < cutoff {
            let _ = fs::remove_file(path);
        }
    }
    let mut remaining = files
        .into_iter()
        .filter(|(path, _, _)| path.exists())
        .collect::<Vec<_>>();
    let mut total = remaining.iter().map(|(_, size, _)| *size).sum::<u64>();
    if total > MAX_ARTIFACT_BYTES {
        remaining.sort_by_key(|(_, _, modified)| *modified);
        for (path, size, _) in remaining {
            if total <= MAX_ARTIFACT_BYTES {
                break;
            }
            if fs::remove_file(path).is_ok() {
                total = total.saturating_sub(size);
            }
        }
    }
}

#[allow(clippy::type_complexity)]
fn known_runtime_artifacts(config: &AgentConfig) -> Vec<(PathBuf, u64, SystemTime)> {
    let output_dir = PathBuf::from(&config.runtime.output_dir);
    let trace_dir = if config.runtime.ai_trace_dir.trim().is_empty() {
        output_dir.join("ai-traces")
    } else {
        PathBuf::from(config.runtime.ai_trace_dir.trim())
    };
    let long_text_dir = if config.wx4py.long_text_file_dir.trim().is_empty() {
        output_dir.join("long-text")
    } else {
        PathBuf::from(config.wx4py.long_text_file_dir.trim())
    };
    let specs: [(PathBuf, fn(&Path) -> bool); 4] = [
        (output_dir.clone(), is_generated_image_artifact),
        (trace_dir, is_ai_trace_artifact),
        (output_dir.join("voice-mp3"), is_voice_mp3_artifact),
        (long_text_dir, is_long_text_artifact),
    ];
    let mut files = Vec::new();
    let mut seen = HashSet::new();
    for (dir, predicate) in specs {
        let Ok(entries) = fs::read_dir(dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if !path.is_file() || !predicate(&path) || !seen.insert(path.clone()) {
                continue;
            }
            let Ok(metadata) = entry.metadata() else {
                continue;
            };
            files.push((
                path,
                metadata.len(),
                metadata.modified().unwrap_or(SystemTime::now()),
            ));
        }
    }
    files
}

fn artifact_name(path: &Path) -> &str {
    path.file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default()
}

fn is_generated_image_artifact(path: &Path) -> bool {
    let name = artifact_name(path);
    name.starts_with("summary-") && name.ends_with(".png")
}

fn is_ai_trace_artifact(path: &Path) -> bool {
    let name = artifact_name(path);
    name.ends_with(".json") && name.contains("-attempt-")
}

fn is_voice_mp3_artifact(path: &Path) -> bool {
    path.extension()
        .and_then(|extension| extension.to_str())
        .is_some_and(|extension| extension.eq_ignore_ascii_case("mp3"))
}

fn is_long_text_artifact(path: &Path) -> bool {
    let name = artifact_name(path);
    name.starts_with("summary-") && name.ends_with(".txt")
}

#[allow(clippy::too_many_arguments)]
async fn handle_platform_event(
    config: &AgentConfig,
    store: &SqliteStateStore,
    matcher: &TriggerMatcher,
    client: &PlatformWorker,
    recent_trigger_attempts: &Arc<Mutex<RecentTriggerAttempts>>,
    recent_observed_messages: &Arc<Mutex<RecentObservedMessages>>,
    image_pipeline_slots: &ImagePipelineSlotPool,
    event_source: PlatformEventSource,
    event: PlatformEvent,
) -> Result<()> {
    let source_platform = event.platform;
    let incoming = IncomingMessage::from(event);
    let Some(trigger) = matcher.match_message(&incoming) else {
        return Ok(());
    };
    let trigger_content_len = trigger.trigger_content.chars().count();
    info!(
        platform = source_platform.as_str(),
        room_id = %trigger.room_id,
        event_source = ?event_source,
        content_len = trigger_content_len,
        "platform trigger event received"
    );

    let Some(command) = parse_summary_command(&trigger, source_platform) else {
        info!(
            room_id = %trigger.room_id,
            content_len = trigger_content_len,
            "trigger-like message ignored because command arguments were not recognized"
        );
        append_runtime_log(
            config,
            &format!(
                "trigger-like message ignored room={} content_len={} reason=unrecognized_command_args",
                trigger.room_id, trigger_content_len
            ),
        );
        return Ok(());
    };

    let observed_realtime = event_source == PlatformEventSource::WxdbRecovered
        && recent_observed_messages
            .lock()
            .map(|recent| {
                recent.has_matching_trigger(
                    &trigger,
                    &incoming,
                    Utc::now(),
                    WXDB_RECOVERED_TRIGGER_REALTIME_DEDUPE_SECONDS,
                )
            })
            .unwrap_or(false);
    if observed_realtime {
        info!(
            room_id = %trigger.room_id,
            content_len = trigger_content_len,
            dedupe_seconds = WXDB_RECOVERED_TRIGGER_REALTIME_DEDUPE_SECONDS,
            "wxdb recovered trigger ignored because realtime listener already observed it"
        );
        append_runtime_log(
            config,
            &format!(
                "wxdb recovered trigger ignored by realtime dedupe room={} content_len={} window_seconds={}",
                trigger.room_id,
                trigger_content_len,
                WXDB_RECOVERED_TRIGGER_REALTIME_DEDUPE_SECONDS
            ),
        );
        return Ok(());
    }

    let duplicate = recent_trigger_attempts
        .lock()
        .map(|mut attempts| {
            attempts.is_duplicate_with_id(
                &trigger,
                incoming.stable_id.as_deref(),
                incoming.timestamp,
            )
        })
        .unwrap_or(false);
    if duplicate {
        info!(
            room_id = %trigger.room_id,
            content_len = trigger_content_len,
            dedupe_window_seconds = TRIGGER_DEDUPE_WINDOW_SECONDS,
            dedupe_event_window_seconds = TRIGGER_DEDUPE_EVENT_WINDOW_SECONDS,
            dedupe_retention_seconds = TRIGGER_DEDUPE_RETENTION_SECONDS,
            "duplicate trigger ignored"
        );
        append_runtime_log(
            config,
            &format!(
                "duplicate trigger ignored room={} content_len={} window_seconds={} event_window_seconds={} retention_seconds={}",
                trigger.room_id,
                trigger_content_len,
                TRIGGER_DEDUPE_WINDOW_SECONDS,
                TRIGGER_DEDUPE_EVENT_WINDOW_SECONDS,
                TRIGGER_DEDUPE_RETENTION_SECONDS
            ),
        );
        return Ok(());
    }
    if !client.supports(command.target_platform) {
        let message = format!(
            "暂不支持从 {} 总结 {} 平台消息。当前已接入的平台：{}。",
            source_platform.as_str(),
            command.target_platform.as_str(),
            client.kind().as_str()
        );
        append_runtime_log(
            config,
            &format!(
                "unsupported target platform room={} source_platform={} target_platform={}",
                trigger.room_id,
                source_platform.as_str(),
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

    let mut pipeline_options =
        PipelineOptions::manual(config, &trigger.room_id, command.image_token_present);
    if config.image_gen.enabled && !config.image_summary_enabled_for_room(&trigger.room_id) {
        info!(room_id = %trigger.room_id, "image summary disabled by room capability");
        append_runtime_log(
            config,
            &format!(
                "image summary disabled by room capability room={}",
                trigger.room_id
            ),
        );
    }
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
    let task = OperationalTask {
        id: store
            .create_task(NewTask {
                room_id: &trigger.room_id,
                source: match event_source {
                    PlatformEventSource::Realtime => "manual",
                    PlatformEventSource::WxdbRecovered => "manual_wxdb_recovered",
                },
                since: range.since,
                until: range.until,
                config_revision: &config_revision(config),
                retry_of: None,
            })?
            .id,
        store: store.clone(),
    };
    task.set_stage(TaskState::Running, "accepted", None, None, 0, 0);

    info!(
        room_id = %trigger.room_id,
        source_platform = source_platform.as_str(),
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
            source_platform.as_str(),
            command.target_platform.as_str(),
            range.since,
            range.until,
            command.range_minutes,
            command.image_token_present
        ),
    );

    let recent_observed_snapshot = recent_observed_messages
        .lock()
        .ok()
        .map(|recent| recent.clone());
    match run_summary_pipeline(SummaryPipelineRequest {
        config,
        client,
        incoming: &incoming,
        trigger: &trigger,
        range: &range,
        options: pipeline_options,
        image_pipeline_slots,
        image_cooldown_recorder: Some(ImageCooldownRecorder {
            store: store.clone(),
            timestamp: incoming.timestamp,
        }),
        recent_observed_messages: recent_observed_snapshot.as_ref(),
        task: Some(&task),
    })
    .await
    {
        Ok(PipelineOutcome::SummaryProduced) => {
            store.set_last_trigger(&trigger.room_id, incoming.timestamp)?;
            record_primary_llm_health(store, config, None);
            task.set_stage(TaskState::Succeeded, "completed", None, None, 0, 0);
            info!(room_id = %trigger.room_id, "summary pipeline completed");
            append_runtime_log(
                config,
                &format!("pipeline completed room={}", trigger.room_id),
            );
        }
        Ok(PipelineOutcome::NoSummary) => {
            task.set_stage(TaskState::Succeeded, "completed_without_output", None, None, 0, 0);
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
            record_primary_llm_health(store, config, Some(&error_message));
            task.set_stage(TaskState::Failed, "failed", None, Some(&error_message), 0, 0);
            error!(room_id = %trigger.room_id, error = %error_message, "summary pipeline failed");
            append_runtime_log(
                config,
                &format!(
                    "pipeline failed room={} error={}",
                    trigger.room_id, error_message
                ),
            );
            let _ = client
                .send_text(
                    &trigger.room_id,
                    &format_failure_message_for_chat("总结失败", &error_message),
                )
                .await;
        }
    }

    Ok(())
}

#[derive(Debug, Default)]
struct RecentTriggerAttempts {
    attempts_by_key: HashMap<String, Vec<RecentTriggerAttempt>>,
}

#[derive(Debug, Clone)]
struct RecentTriggerAttempt {
    stable_id: Option<String>,
    event_at: DateTime<Utc>,
    observed_at: DateTime<Utc>,
}

impl RecentTriggerAttempts {
    #[cfg(test)]
    fn is_duplicate_at(
        &mut self,
        trigger: &TriggerMatch,
        event_at: DateTime<Utc>,
        observed_at: DateTime<Utc>,
    ) -> bool {
        self.is_duplicate_at_with_id(trigger, None, event_at, observed_at)
    }

    fn is_duplicate_with_id(
        &mut self,
        trigger: &TriggerMatch,
        stable_id: Option<&str>,
        event_at: DateTime<Utc>,
    ) -> bool {
        self.is_duplicate_at_with_id(trigger, stable_id, event_at, Utc::now())
    }

    fn is_duplicate_at_with_id(
        &mut self,
        trigger: &TriggerMatch,
        stable_id: Option<&str>,
        event_at: DateTime<Utc>,
        observed_at: DateTime<Utc>,
    ) -> bool {
        let retention_cutoff = observed_at - Duration::seconds(TRIGGER_DEDUPE_RETENTION_SECONDS);
        self.attempts_by_key.retain(|_, attempts| {
            attempts.retain(|attempt| attempt.observed_at >= retention_cutoff);
            !attempts.is_empty()
        });

        let process_cutoff = observed_at - Duration::seconds(TRIGGER_DEDUPE_WINDOW_SECONDS);
        let key = trigger_attempt_key(trigger);
        if let Some(attempts) = self.attempts_by_key.get(&key) {
            let duplicate = attempts.iter().any(|attempt| {
                match (stable_id, attempt.stable_id.as_deref()) {
                    // When both transports provide stable IDs, distinct IDs
                    // represent distinct commands even if they arrive close together.
                    (Some(current), Some(previous)) => stable_ids_match(previous, current),
                    // wx4py realtime events can omit their ID while wxdb has one
                    // (and vice versa). Fall back to the room/content key and a
                    // bounded arrival-time window so the late replay is ignored.
                    _ => {
                        attempt.observed_at >= process_cutoff
                            || event_times_close(attempt.event_at, event_at)
                    }
                }
            });
            if duplicate {
                return true;
            }
        }

        self.attempts_by_key
            .entry(key)
            .or_default()
            .push(RecentTriggerAttempt {
                stable_id: stable_id.map(ToOwned::to_owned),
                event_at,
                observed_at,
            });
        false
    }
}

fn event_times_close(left: DateTime<Utc>, right: DateTime<Utc>) -> bool {
    let delta = (left - right).num_seconds();
    (-TRIGGER_DEDUPE_EVENT_WINDOW_SECONDS..=TRIGGER_DEDUPE_EVENT_WINDOW_SECONDS).contains(&delta)
}

fn trigger_attempt_key(trigger: &TriggerMatch) -> String {
    format!(
        "{}\n{}",
        trigger.room_id.trim(),
        trigger.trigger_content.trim()
    )
}

#[derive(Debug, Clone, Default)]
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
                    && !history_rules::is_current_incoming(message, incoming)
                    && !history_rules::is_agent_status_content(&message.content)
            })
            .count()
    }

    fn has_matching_trigger(
        &self,
        trigger: &TriggerMatch,
        incoming: &IncomingMessage,
        now: DateTime<Utc>,
        window_seconds: i64,
    ) -> bool {
        let cutoff = now - Duration::seconds(window_seconds);
        let target_content = trigger.trigger_content.trim();
        self.messages.iter().any(|message| {
            let same_stable_id = message
                .stable_id
                .as_deref()
                .zip(incoming.stable_id.as_deref())
                .is_some_and(|(left, right)| stable_ids_match(left, right));
            message.timestamp >= cutoff
                && message.room_id == trigger.room_id
                && message.msg_type == "text"
                && !message.is_self
                && (same_stable_id
                    || (incoming.stable_id.is_none()
                        && message.stable_id.is_none()
                        && message.content.trim() == target_content))
        })
    }

    fn prune(&mut self, now: DateTime<Utc>) {
        let cutoff = now - Duration::hours(RECENT_OBSERVED_WINDOW_HOURS);
        self.messages.retain(|message| message.timestamp >= cutoff);
        while self.messages.len() > RECENT_OBSERVED_MAX_MESSAGES {
            self.messages.pop_front();
        }
    }
}

fn drain_scheduled_backlog(
    config: &AgentConfig,
    store: &SqliteStateStore,
    client: &PlatformWorker,
    image_pipeline_slots: &ImagePipelineSlotPool,
    backlog: &mut ScheduledSummaryBacklog,
    scheduler: &mut SummaryTaskScheduler,
) {
    if backlog.is_empty() || !backlog.retry_ready(Instant::now()) {
        return;
    }

    let mut queue_full = false;
    while let Some(request) = backlog.requests.pop_front() {
        let room = request.room_id.clone();
        let now = request.due_at;
        if !config.scheduled_summary.ignore_rate_limit {
            let last_trigger = match store.get_last_trigger(&room) {
                Ok(value) => value,
                Err(error) => {
                    append_runtime_log(
                        config,
                        &format!("scheduled summary state read failed room={room} error={error}"),
                    );
                    requeue_scheduled_request_after_state_read_failure(backlog, request);
                    return;
                }
            };
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
            stable_id: None,
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
        let task_config = config.clone();
        let task_store = store.clone();
        let task_client = client.clone();
        let task_image_pipeline_slots = image_pipeline_slots.clone();
        let task_range = request.range.clone();
        let future = Box::pin(async move {
            run_scheduled_summary_task(
                &task_config,
                &task_store,
                &task_client,
                incoming,
                trigger,
                task_range,
                now,
                &task_image_pipeline_slots,
            )
            .await
        });
        match scheduler.enqueue(room.clone(), future) {
            ScheduleResult::Started | ScheduleResult::Queued => {}
            ScheduleResult::DuplicateRoom => {
                backlog.requests.push_back(request);
                append_runtime_log(
                    config,
                    &format!("scheduled summary pending room={room} reason=in_flight"),
                );
            }
            ScheduleResult::QueueFull => {
                queue_full = true;
                backlog.requests.push_back(request);
                append_runtime_log(
                    config,
                    &format!("scheduled summary pending room={room} reason=queue_full"),
                );
            }
        }
    }
    if queue_full || !backlog.requests.is_empty() {
        backlog.record_retry(Instant::now());
    } else {
        backlog.clear_retry();
    }
}

fn requeue_scheduled_request_after_state_read_failure(
    backlog: &mut ScheduledSummaryBacklog,
    request: ScheduledSummaryRequest,
) {
    backlog.requests.push_front(request);
    backlog.record_retry(Instant::now());
}

fn drain_manual_retry_tasks(
    config: &AgentConfig,
    store: &SqliteStateStore,
    client: &PlatformWorker,
    image_pipeline_slots: &ImagePipelineSlotPool,
    scheduler: &mut SummaryTaskScheduler,
) {
    let retries = match store.queued_retry_tasks(8) {
        Ok(tasks) => tasks,
        Err(error) => {
            append_runtime_log(config, &format!("manual retry task query failed error={error}"));
            return;
        }
    };
    for record in retries {
        let room_id = record.room_id.clone();
        let scheduler_room_id = room_id.clone();
        let task = OperationalTask { id: record.id.clone(), store: store.clone() };
        let task_config = config.clone();
        let task_client = client.clone();
        let task_slots = image_pipeline_slots.clone();
        let task_for_future = task.clone();
        let future = Box::pin(async move {
            task_for_future.set_stage(TaskState::Running, "retry_accepted", None, None, 0, 0);
            let incoming = IncomingMessage {
                room_id: room_id.clone(), room_name: Some(room_id.clone()), stable_id: None,
                sender_id: "task_center".to_string(), sender_name: Some("任务中心重试".to_string()),
                content: "[task_retry]".to_string(), msg_type: "text".to_string(), timestamp: Utc::now(), is_self: true,
            };
            let trigger = TriggerMatch { room_id: room_id.clone(), trigger_symbol: "[task_retry]".to_string(), trigger_content: "[task_retry]".to_string() };
            let range = ResolvedTimeRange { since: record.since, until: record.until, mode: TimeRangeMode::FixedMinutes };
            let result = run_summary_pipeline(SummaryPipelineRequest {
                config: &task_config,
                client: &task_client,
                incoming: &incoming,
                trigger: &trigger,
                range: &range,
                options: PipelineOptions::manual(&task_config, &room_id, false),
                image_pipeline_slots: &task_slots,
                image_cooldown_recorder: Some(ImageCooldownRecorder { store: task_for_future.store.clone(), timestamp: Utc::now() }),
                recent_observed_messages: None,
                task: Some(&task_for_future),
            }).await;
            match &result {
                Ok(PipelineOutcome::SummaryProduced) => task_for_future.set_stage(TaskState::Succeeded, "retry_completed", None, None, 0, 0),
                Ok(PipelineOutcome::NoSummary) => task_for_future.set_stage(TaskState::Succeeded, "retry_completed_without_output", None, None, 0, 0),
                Err(error) => task_for_future.set_stage(TaskState::Failed, "retry_failed", None, Some(&format_error_chain(error)), 0, 0),
            }
            result.map(|_| ())
        });
        match scheduler.enqueue(scheduler_room_id, future) {
            ScheduleResult::Started | ScheduleResult::Queued => {}
            ScheduleResult::DuplicateRoom | ScheduleResult::QueueFull => break,
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_scheduled_summary_task(
    config: &AgentConfig,
    store: &SqliteStateStore,
    client: &PlatformWorker,
    incoming: IncomingMessage,
    trigger: TriggerMatch,
    range: ResolvedTimeRange,
    now: DateTime<Utc>,
    image_pipeline_slots: &ImagePipelineSlotPool,
) -> Result<()> {
    if config.image_gen.enabled && !config.image_summary_enabled_for_room(&trigger.room_id) {
        info!(room_id = %trigger.room_id, "scheduled image summary disabled by room capability");
        append_runtime_log(
            config,
            &format!(
                "scheduled image summary disabled by room capability room={}",
                trigger.room_id
            ),
        );
    }
    let task = OperationalTask {
        id: store
            .create_task(NewTask {
                room_id: &trigger.room_id,
                source: "scheduled",
                since: range.since,
                until: range.until,
                config_revision: &config_revision(config),
                retry_of: None,
            })?
            .id,
        store: store.clone(),
    };
    task.set_stage(TaskState::Running, "accepted", None, None, 0, 0);
    match run_summary_pipeline(SummaryPipelineRequest {
        config,
        client,
        incoming: &incoming,
        trigger: &trigger,
        range: &range,
        options: PipelineOptions::scheduled(config, &trigger.room_id),
        image_pipeline_slots,
        image_cooldown_recorder: None,
        recent_observed_messages: None,
        task: Some(&task),
    })
    .await
    {
        Ok(PipelineOutcome::SummaryProduced) => {
            store.set_last_trigger(&trigger.room_id, now)?;
            record_primary_llm_health(store, config, None);
            task.set_stage(TaskState::Succeeded, "completed", None, None, 0, 0);
            append_runtime_log(
                config,
                &format!("scheduled pipeline completed room={}", trigger.room_id),
            );
            Ok(())
        }
        Ok(PipelineOutcome::NoSummary) => {
            task.set_stage(TaskState::Succeeded, "completed_without_output", None, None, 0, 0);
            append_runtime_log(
                config,
                &format!(
                    "scheduled pipeline completed without summary room={}",
                    trigger.room_id
                ),
            );
            Ok(())
        }
        Err(error) => {
            let error_message = format_error_chain(&error);
            record_primary_llm_health(store, config, Some(&error_message));
            task.set_stage(TaskState::Failed, "failed", None, Some(&error_message), 0, 0);
            append_runtime_log(
                config,
                &format!(
                    "scheduled pipeline failed room={} error={error_message}",
                    trigger.room_id
                ),
            );
            let _ = client
                .send_text(
                    &trigger.room_id,
                    &format_failure_message_for_chat("定时总结失败", &error_message),
                )
                .await;
            Err(error)
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct PipelineOptions {
    text_summary_enabled: bool,
    image_gen_enabled: bool,
    send_progress: bool,
    defer_text_until_image_ready: bool,
    send_disabled_message: bool,
    log_retry_attempts: bool,
}

#[derive(Clone)]
struct ImageCooldownRecorder {
    store: SqliteStateStore,
    timestamp: DateTime<Utc>,
}

struct SummaryPipelineRequest<'a> {
    config: &'a AgentConfig,
    client: &'a PlatformWorker,
    incoming: &'a IncomingMessage,
    trigger: &'a TriggerMatch,
    range: &'a ResolvedTimeRange,
    options: PipelineOptions,
    image_pipeline_slots: &'a ImagePipelineSlotPool,
    image_cooldown_recorder: Option<ImageCooldownRecorder>,
    recent_observed_messages: Option<&'a RecentObservedMessages>,
    task: Option<&'a OperationalTask>,
}

struct PreparedPipelineInput {
    chat_messages: Vec<ChatMessage>,
    privacy: PrivacyFilter,
    llm_input: String,
    total_messages: usize,
    media: media_service::MediaEnrichment,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum PipelineOutcome {
    SummaryProduced,
    NoSummary,
}

impl PipelineOptions {
    fn manual(config: &AgentConfig, room_id: &str, image_token_present: bool) -> Self {
        let image_enabled_for_request =
            config.manual_summary.image_by_default ^ image_token_present;
        Self {
            text_summary_enabled: config.text_summary.enabled,
            image_gen_enabled: config.image_gen.enabled
                && config.image_summary_enabled_for_room(room_id)
                && image_enabled_for_request,
            send_progress: true,
            defer_text_until_image_ready: false,
            send_disabled_message: true,
            log_retry_attempts: true,
        }
    }

    fn scheduled(config: &AgentConfig, room_id: &str) -> Self {
        Self {
            text_summary_enabled: config.scheduled_summary.send_text && config.text_summary.enabled,
            image_gen_enabled: config.scheduled_summary.send_image
                && config.image_gen.enabled
                && config.image_summary_enabled_for_room(room_id),
            send_progress: false,
            defer_text_until_image_ready: true,
            send_disabled_message: false,
            log_retry_attempts: false,
        }
    }
}

fn image_pipeline_slot_capacity(config: &AgentConfig, platform_rooms: &[String]) -> usize {
    let enabled_rooms = if config.scheduled_summary.enabled
        && config.scheduled_summary.send_image
        && config.image_gen.enabled
    {
        scheduled_rooms(config, platform_rooms)
            .into_iter()
            .filter(|room_id| config.image_summary_enabled_for_room(room_id))
            .count()
    } else {
        0
    };
    let automatic = enabled_rooms.max(1);
    let configured = config.image_pipeline.max_concurrent_requests;
    if configured == 0 {
        automatic
    } else {
        automatic.min(configured.max(1))
    }
}

#[allow(clippy::too_many_arguments)]
async fn query_platform_history_paginated(
    config: &AgentConfig,
    client: &PlatformWorker,
    room_id: &str,
    room_name: Option<&str>,
    since: DateTime<Utc>,
    until: DateTime<Utc>,
    page_limit: usize,
    media_decode_limit: Option<usize>,
) -> Result<Vec<PlatformHistoryMessage>> {
    let page_limit = page_limit.max(1);
    let query_limit = page_limit.min(u32::MAX as usize) as u32;
    let mut page_until = until;
    let mut cursor: Option<PlatformHistoryCursor> = None;
    let mut pages = 0usize;
    let mut history = Vec::new();
    let mut seen = HashSet::new();
    let mut remaining_media_decode_limit = media_decode_limit;

    loop {
        pages += 1;
        let page = client
            .query_text_messages(
                room_id,
                room_name,
                since,
                page_until,
                query_limit,
                remaining_media_decode_limit,
                cursor.as_ref(),
            )
            .await
            .with_context(|| format!("querying platform chat history page {pages}"))?;
        let page_len = page.len();
        let oldest = page
            .iter()
            .min_by(|left, right| platform_history_order(left, right))
            .map(|message| (message.timestamp, message.stable_id.clone()));
        let first_ts = oldest.as_ref().map(|(timestamp, _)| *timestamp);
        let last_ts = page.iter().map(|message| message.timestamp).max();
        let decoded_media_count = media_decode_attempt_count(&page);
        if let Some(remaining) = &mut remaining_media_decode_limit {
            *remaining = remaining.saturating_sub(decoded_media_count);
        }

        let before_len = history.len();
        for message in page {
            if seen.insert(platform_history_message_key(&message)) {
                history.push(message);
            }
        }
        let new_messages = history.len().saturating_sub(before_len);

        if pages == 1 || pages.is_multiple_of(10) || page_len < page_limit || first_ts < Some(since)
        {
            info!(
                room_id = %room_id,
                page = pages,
                page_len,
                new_messages,
                total = history.len(),
                first = ?first_ts,
                last = ?last_ts,
                page_limit,
                "platform history page completed"
            );
            append_runtime_log(
                config,
                &format!(
                    "history page completed room={} page={} page_len={} new={} total={} first={} last={} page_limit={}",
                    room_id,
                    pages,
                    page_len,
                    new_messages,
                    history.len(),
                    first_ts
                        .map(|value| value.to_rfc3339())
                        .unwrap_or_else(|| "-".to_string()),
                    last_ts
                        .map(|value| value.to_rfc3339())
                        .unwrap_or_else(|| "-".to_string()),
                    page_limit
                ),
            );
        }

        if page_len == 0 || page_len < page_limit || first_ts < Some(since) {
            break;
        }

        let Some((oldest_timestamp, stable_id)) = oldest else {
            break;
        };
        let Some(stable_id) = stable_id else {
            anyhow::bail!(
                "platform history page is full but oldest message has no stable ID; refusing unsafe timestamp-only pagination"
            );
        };
        let next_cursor = PlatformHistoryCursor {
            timestamp: oldest_timestamp,
            stable_id,
        };

        if new_messages == 0 || cursor.as_ref() == Some(&next_cursor) {
            warn!(
                room_id = %room_id,
                page = pages,
                page_until = %page_until,
                next_until = %next_cursor.timestamp,
                "stopping paginated history query because it made no backward progress"
            );
            append_runtime_log(
                config,
                &format!(
                    "history pagination stopped without progress room={} page={} page_until={} next_until={}",
                    room_id, pages, page_until, next_cursor.timestamp
                ),
            );
            break;
        }

        page_until = next_cursor.timestamp;
        cursor = Some(next_cursor);
    }

    history.sort_by(platform_history_order);
    info!(
        room_id = %room_id,
        pages,
        history_len = history.len(),
        page_limit,
        "platform history paginated query completed"
    );
    append_runtime_log(
        config,
        &format!(
            "history paginated query completed room={} pages={} history_len={} page_limit={}",
            room_id,
            pages,
            history.len(),
            page_limit
        ),
    );

    Ok(history)
}

fn platform_history_message_key(message: &PlatformHistoryMessage) -> String {
    if let Some(stable_id) = &message.stable_id {
        return format!("stable:{stable_id}");
    }
    format!(
        "{}|{}|{}|{}|{}|{}|{}|{}",
        message.timestamp.timestamp_millis(),
        message.sender_id,
        message.sender_name.as_deref().unwrap_or(""),
        message.msg_type,
        message.content,
        message.media_path.as_deref().unwrap_or(""),
        message.thumbnail_path.as_deref().unwrap_or(""),
        message.decoded_media_path.as_deref().unwrap_or("")
    )
}

fn platform_history_order(
    left: &PlatformHistoryMessage,
    right: &PlatformHistoryMessage,
) -> Ordering {
    left.timestamp.cmp(&right.timestamp).then_with(|| {
        match (
            left.stable_id.as_deref().and_then(stable_id_number),
            right.stable_id.as_deref().and_then(stable_id_number),
        ) {
            (Some(left), Some(right)) => left.cmp(&right),
            _ => left.stable_id.cmp(&right.stable_id),
        }
    })
}

fn stable_id_number(value: &str) -> Option<u64> {
    value
        .rsplit_once(':')
        .map(|(_, id)| id)
        .unwrap_or(value)
        .parse()
        .ok()
}

fn stable_ids_match(left: &str, right: &str) -> bool {
    left == right
        || stable_id_number(left).is_some() && stable_id_number(left) == stable_id_number(right)
}

fn media_decode_attempt_count(messages: &[PlatformHistoryMessage]) -> usize {
    messages
        .iter()
        .filter(|message| {
            message.decoded_media_path.is_some() || message.media_decode_error.is_some()
        })
        .count()
}

async fn load_summary_history(
    config: &AgentConfig,
    client: &PlatformWorker,
    incoming: &IncomingMessage,
    trigger: &TriggerMatch,
    range: &ResolvedTimeRange,
    recent_observed_messages: Option<&RecentObservedMessages>,
) -> Result<Option<Vec<PlatformHistoryMessage>>> {
    let history_page_limit = config.history_message_limit();
    let media_decode_limit = summary_media_decode_limit(config);
    info!(
        room_id = %trigger.room_id,
        since = %range.since,
        until = %range.until,
        page_limit = history_page_limit,
        media_decode_limit = ?media_decode_limit,
        "querying platform history"
    );
    append_runtime_log(
        config,
        &format!(
            "history query started room={} since={} until={} page_limit={} media_decode_limit={}",
            trigger.room_id,
            range.since,
            range.until,
            history_page_limit,
            format_media_decode_limit(media_decode_limit)
        ),
    );
    let mut history = query_platform_history_paginated(
        config,
        client,
        &trigger.room_id,
        incoming.room_name.as_deref(),
        range.since,
        range.until,
        history_page_limit,
        media_decode_limit,
    )
    .await
    .context("querying platform chat history")?;
    if !history.is_empty() {
        return Ok(Some(history));
    }

    let observed_count = recent_observed_messages
        .map(|recent| {
            recent.count_user_text_in_range(&trigger.room_id, range.since, range.until, incoming)
        })
        .unwrap_or(0);
    if observed_count == 0 {
        return Ok(Some(history));
    }

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
    for (retry_index, delay_ms) in EMPTY_HISTORY_RETRY_DELAYS_MS.iter().copied().enumerate() {
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
        history = query_platform_history_paginated(
            config,
            client,
            &trigger.room_id,
            incoming.room_name.as_deref(),
            range.since,
            range.until,
            history_page_limit,
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
            return Ok(Some(history));
        }
    }

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
    Ok(None)
}

async fn prepare_pipeline_input(
    config: &AgentConfig,
    client: &PlatformWorker,
    room_id: &str,
    options: PipelineOptions,
    task: Option<&OperationalTask>,
    mut history: Vec<PlatformHistoryMessage>,
) -> Result<Option<PreparedPipelineInput>> {
    let media = media_service::enrich_history(config, room_id, &mut history).await?;
    let prepared = summary_input::prepare(config, history);
    let chat_messages = prepared.messages;
    if let Some(task) = task {
        summary_input::persist_sources(task, &chat_messages);
        task.set_stage(
            TaskState::Running,
            "media_processing",
            None,
            None,
            chat_messages.len() as u64,
            media.total() as u64,
        );
    }
    if prepared.total_messages == 0 {
        client
            .send_text(room_id, "这段时间没有可总结的文本聊天记录。")
            .await
            .context("sending empty-history message")?;
        return Ok(None);
    }

    info!(
        room_id,
        input_chars = prepared.llm_input.chars().count(),
        total_messages = prepared.total_messages,
        text_summary_enabled = options.text_summary_enabled,
        image_gen_enabled = options.image_gen_enabled,
        "LLM input prepared"
    );
    append_runtime_log(
        config,
        &format!(
            "llm input prepared room={} input_chars={} messages={} text_enabled={} image_enabled={}",
            room_id,
            prepared.llm_input.chars().count(),
            prepared.total_messages,
            options.text_summary_enabled,
            options.image_gen_enabled
        ),
    );
    for (label, count) in [
        ("image captions", media.images),
        ("video captions", media.videos),
        ("voice transcriptions", media.voices),
    ] {
        if count > 0 {
            append_runtime_log(
                config,
                &format!("{} inserted room={} count={}", label, room_id, count),
            );
        }
    }

    Ok(Some(PreparedPipelineInput {
        chat_messages,
        privacy: prepared.privacy,
        llm_input: prepared.llm_input,
        total_messages: prepared.total_messages,
        media,
    }))
}

async fn run_summary_pipeline(request: SummaryPipelineRequest<'_>) -> Result<PipelineOutcome> {
    let SummaryPipelineRequest {
        config,
        client,
        incoming,
        trigger,
        range,
        options,
        image_pipeline_slots,
        image_cooldown_recorder,
        recent_observed_messages,
        task,
    } = request;
    if task.is_some_and(OperationalTask::cancelled) {
        return Ok(PipelineOutcome::NoSummary);
    }
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
        if let Err(error) = client
            .send_text(&trigger.room_id, progress_message(options))
            .await
        {
            warn!(
                room_id = %trigger.room_id,
                error = %error,
                "progress message delivery failed; continuing summary pipeline"
            );
            append_runtime_log(
                config,
                &format!(
                    "progress message delivery failed room={} action=continue error={error:#}",
                    trigger.room_id
                ),
            );
        }
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

    let retry_notifier = options
        .log_retry_attempts
        .then(|| retry_log_notifier(config, trigger.room_id.clone()));

    let Some(mut history) = load_summary_history(
        config,
        client,
        incoming,
        trigger,
        range,
        recent_observed_messages,
    )
    .await?
    else {
        return Ok(PipelineOutcome::NoSummary);
    };
    let platform_history_len = history.len();
    if let Some(task) = task {
        task.set_stage(TaskState::Running, "history", None, None, platform_history_len as u64, 0);
    }
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
        !history_rules::is_current_trigger(message, incoming) && !history_rules::is_agent_status(message)
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

    let Some(prepared_input) = prepare_pipeline_input(
        config,
        client,
        &trigger.room_id,
        options,
        task,
        history,
    )
    .await?
    else {
        return Ok(PipelineOutcome::NoSummary);
    };
    let chat_messages = prepared_input.chat_messages;
    let privacy = prepared_input.privacy;
    let llm_input = prepared_input.llm_input;
    let total_messages = prepared_input.total_messages;
    let media = prepared_input.media;
    let mut llm = configure_llm_tracing(
        OpenAiCompatibleLlm::new(config.llm.clone(), &config.proxy)
            .context("initializing LLM client")?,
        config,
    )
    .context("configuring LLM trace output")?;
    if let Some(retry_notifier) = retry_notifier.clone() {
        llm = llm.with_retry_notifier(retry_notifier);
    }
    // Text summaries can use the configured SSE transport. The two chat
    // completions that prepare image generation must be regular JSON replies:
    // some compatible providers close long SSE bodies with invalid encoding.
    let image_llm = llm.clone().with_streaming(false);
    let mut pending_text_reply = None;
    if options.text_summary_enabled {
        if let Some(task) = task {
            task.set_stage(
                TaskState::Running,
                "text_summary",
                None,
                None,
                chat_messages.len() as u64,
                media.total() as u64,
            );
        }
        let summary_result = llm_service::complete_text_summary_with_refusal_retry(
            config,
            &llm,
            &trigger.room_id,
            &chat_messages,
            &privacy,
            TEXT_SUMMARY_REFUSAL_RETRY_PROMPT,
        )
        .await
        .context("calling LLM for text summary")?;
        let summary = summary_result.output;
        if let Some(task) = task {
            task.set_stage(
                TaskState::Running,
                "text_summary_completed",
                Some(&summary),
                None,
                chat_messages.len() as u64,
                media.total() as u64,
            );
        }
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
        let reply = format_summary_reply(&summary, range, total_messages);
        if options.defer_text_until_image_ready {
            pending_text_reply = Some(reply);
        } else {
            if let Some(task) = task {
                deliver_outboxed_text(config, task, client, &trigger.room_id, &reply).await?;
            } else {
                client
                    .send_text(&trigger.room_id, &reply)
                    .await
                    .context("sending summary text")?;
            }
            info!(room_id = %trigger.room_id, "summary text sent");
            append_runtime_log(
                config,
                &format!("summary text sent room={}", trigger.room_id),
            );
        }
    }

    if options.image_gen_enabled && !options.defer_text_until_image_ready {
        let image_sent = pipeline_delivery::run_background_image_pipeline(
            config.clone(),
            client.sender(),
            trigger.room_id.clone(),
            llm_input,
            chat_messages,
            options.text_summary_enabled,
            image_pipeline_slots.clone(),
            image_cooldown_recorder,
        )
        .await;
        info!(
            room_id = %trigger.room_id,
            image_sent,
            "manual image pipeline completed"
        );
        append_runtime_log(
            config,
            &format!("manual image pipeline completed room={}", trigger.room_id),
        );
        return Ok(if image_sent || options.text_summary_enabled {
            PipelineOutcome::SummaryProduced
        } else {
            PipelineOutcome::NoSummary
        });
    }

    if options.image_gen_enabled {
        let image_prompt = match summary_image::prepare_foreground_prompt(
            config,
            &image_llm,
            &trigger.room_id,
            &llm_input,
            &chat_messages,
            &privacy,
            image_pipeline_slots,
            IMAGE_PIPELINE_REFUSAL_RETRY_PROMPT,
        )
        .await
        {
            Ok(prompt) => prompt,
            Err(summary_image::ForegroundPromptPreparationError::Summary(error)) => {
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
                pipeline_delivery::send_deferred_summary_text(
                    config,
                    client,
                    &trigger.room_id,
                    &mut pending_text_reply,
                    "after image summary failure",
                    task,
                )
                .await?;
                if options.text_summary_enabled {
                    pipeline_delivery::send_image_failure_message(
                        config,
                        client,
                        &trigger.room_id,
                        &error_message,
                    )
                        .await;
                    return Ok(PipelineOutcome::SummaryProduced);
                }
                return Err(error);
            }
            Err(summary_image::ForegroundPromptPreparationError::Prompt(error)) => {
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
                pipeline_delivery::send_deferred_summary_text(
                    config,
                    client,
                    &trigger.room_id,
                    &mut pending_text_reply,
                    "after image prompt failure",
                    task,
                )
                .await?;
                if options.text_summary_enabled {
                    pipeline_delivery::send_image_failure_message(
                        config,
                        client,
                        &trigger.room_id,
                        &error_message,
                    )
                        .await;
                    return Ok(PipelineOutcome::SummaryProduced);
                }
                return Err(error);
            }
        };

        match summary_image::generate(
            config,
            &trigger.room_id,
            &image_prompt,
            retry_notifier.clone(),
        )
        .await
        {
            Ok(artifact) => {
                pipeline_delivery::send_deferred_summary_text(
                    config,
                    client,
                    &trigger.room_id,
                    &mut pending_text_reply,
                    "before image send",
                    task,
                )
                .await?;
                if let Some(task) = task {
                    deliver_outboxed_image(config, task, client, &trigger.room_id, &artifact).await?;
                } else {
                    summary_image::send_with_worker(config, client, &trigger.room_id, &artifact).await?;
                }
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
                pipeline_delivery::send_deferred_summary_text(
                    config,
                    client,
                    &trigger.room_id,
                    &mut pending_text_reply,
                    "after image generation failure",
                    task,
                )
                .await?;
                if options.text_summary_enabled {
                    pipeline_delivery::send_image_failure_message(
                        config,
                        client,
                        &trigger.room_id,
                        &error_message,
                    )
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
                        &format!(
                            "{prefix}{}",
                            format_failure_message_for_chat("图片生成失败", &error_message)
                        ),
                    )
                    .await
                    .context("sending image failure message")?;
                return Ok(PipelineOutcome::NoSummary);
            }
        }
    }

    pipeline_delivery::send_deferred_summary_text(
        config,
        client,
        &trigger.room_id,
        &mut pending_text_reply,
        "without image",
        task,
    )
    .await?;

    Ok(PipelineOutcome::SummaryProduced)
}

async fn deliver_outboxed_text(
    config: &AgentConfig,
    task: &OperationalTask,
    client: &PlatformWorker,
    room_id: &str,
    text: &str,
) -> Result<()> { outbox::deliver_text(config, task, client, room_id, text).await }

async fn deliver_outboxed_image(
    config: &AgentConfig,
    task: &OperationalTask,
    client: &PlatformWorker,
    room_id: &str,
    artifact: &ImageArtifact,
) -> Result<()> { outbox::deliver_image(config, task, client, room_id, artifact).await }

async fn drain_outbox(
    config: &AgentConfig,
    store: &SqliteStateStore,
    client: &PlatformWorker,
) -> Result<()> { outbox::drain(config, store, client).await }

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

fn config_revision(config: &AgentConfig) -> String {
    format!(
        "{}:{}:{}:{}",
        config.platform.kind.as_str(),
        config.llm.provider,
        config.llm.model.as_deref().unwrap_or_default(),
        config.runtime.output_dir
    )
}

fn record_primary_llm_health(store: &SqliteStateStore, config: &AgentConfig, error: Option<&str>) {
    let provider_key = format!(
        "{}:{}",
        config.llm.provider,
        config.llm.model.as_deref().unwrap_or("default")
    );
    let failures = if error.is_some() {
        store
            .provider_health()
            .ok()
            .and_then(|entries| entries.into_iter().find(|entry| entry.capability == "llm" && entry.provider_key == provider_key))
            .map(|entry| entry.consecutive_failures.saturating_add(1))
            .unwrap_or(1)
    } else { 0 };
    let circuit_open_until = (failures >= 3).then(|| Utc::now() + Duration::minutes(5));
    if let Err(update_error) = store.update_provider_health("llm", &provider_key, failures, circuit_open_until, error) {
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
            id: store.create_task(NewTask {
                room_id,
                source: "weekly_report",
                since,
                until: Utc::now(),
                config_revision: &config_revision(config),
                retry_of: None,
            })?.id,
            store: store.clone(),
        };
        task.set_stage(TaskState::Running, "weekly_chart", Some(&stats), None, weekly.tasks as u64, weekly.media);
        let outcome = async {
            let artifact = summary_image::generate(config, room_id, &stats, None).await?;
            deliver_outboxed_image(config, &task, client, room_id, &artifact).await?;
            let caption = format!("群组周报：近 7 天共 {} 次总结，成功 {} 次，失败 {} 次。", weekly.tasks, weekly.succeeded, weekly.failed);
            deliver_outboxed_text(config, &task, client, room_id, &caption).await?;
            store.update_weekly_metric(&report_id, "succeeded", Some(&caption), Some(&artifact.path), None)?;
            task.set_stage(TaskState::Succeeded, "weekly_chart_delivered", Some(&caption), None, weekly.tasks as u64, weekly.media);
            Ok::<(), anyhow::Error>(())
        }.await;
        if let Err(error) = outcome {
            let error_message = format_error_chain(&error);
            let caption = format!("群组周报：近 7 天共 {} 次总结，成功 {} 次，失败 {} 次。统计图生成失败，已降级为文字统计。", weekly.tasks, weekly.succeeded, weekly.failed);
            let _ = deliver_outboxed_text(config, &task, client, room_id, &caption).await;
            store.update_weekly_metric(&report_id, "degraded", Some(&caption), None, Some(&error_message))?;
            task.set_stage(TaskState::Succeeded, "weekly_text_fallback", Some(&caption), Some(&error_message), weekly.tasks as u64, weekly.media);
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

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
struct SummaryCommand {
    target_platform: PlatformKindConfig,
    range_minutes: Option<i64>,
    image_token_present: bool,
}

fn parse_summary_command(
    trigger: &TriggerMatch,
    default_platform: PlatformKindConfig,
) -> Option<SummaryCommand> {
    let args = trigger
        .trigger_content
        .strip_prefix(&trigger.trigger_symbol)
        .unwrap_or_default()
        .trim();
    parse_summary_command_args(args, default_platform)
}

fn parse_summary_command_args(
    args: &str,
    default_platform: PlatformKindConfig,
) -> Option<SummaryCommand> {
    let args = args.trim();
    if args.is_empty() {
        return Some(SummaryCommand {
            target_platform: default_platform,
            range_minutes: None,
            image_token_present: false,
        });
    }

    let mut target_platform = default_platform;
    let mut image_token_present = false;
    let mut range_tokens: Vec<&str> = Vec::new();
    for token in args.split_whitespace() {
        if let Some(platform) = parse_platform_token(token) {
            target_platform = platform;
        } else if is_image_token(token) {
            image_token_present = true;
        } else {
            range_tokens.push(token);
        }
    }

    let range_minutes = if range_tokens.is_empty() {
        None
    } else {
        parse_summary_time_range_minutes(&range_tokens)?
    };

    Some(SummaryCommand {
        target_platform,
        range_minutes,
        image_token_present,
    })
}

fn parse_platform_token(token: &str) -> Option<PlatformKindConfig> {
    PlatformKindConfig::parse_alias(token)
}

fn is_image_token(token: &str) -> bool {
    let token = token.trim();
    matches!(token, "图片") || matches!(token.to_ascii_lowercase().as_str(), "image" | "img")
}

fn parse_summary_time_range_minutes(tokens: &[&str]) -> Option<Option<i64>> {
    if tokens.len() == 1 && is_default_time_range_token(tokens[0]) {
        return Some(None);
    }
    parse_strict_duration_minutes(tokens).map(Some)
}

fn is_default_time_range_token(token: &str) -> bool {
    let token = token.trim();
    matches!(token.to_ascii_lowercase().as_str(), "today")
        || matches!(token, "今天" | "今日" | "本日")
}

fn parse_strict_duration_minutes(tokens: &[&str]) -> Option<i64> {
    if tokens.is_empty() || tokens.len() > 2 {
        return None;
    }

    let first = tokens[0].trim();
    if first.is_empty() {
        return None;
    }

    let split_at = first
        .char_indices()
        .find(|(_, ch)| !ch.is_ascii_digit())
        .map(|(index, _)| index)
        .unwrap_or(first.len());
    if split_at == 0 {
        return None;
    }

    let (amount, inline_unit) = first.split_at(split_at);
    let amount = amount.parse::<i64>().ok()?;
    if amount <= 0 {
        return None;
    }

    let unit = if inline_unit.is_empty() {
        if tokens.len() != 2 {
            return None;
        }
        tokens[1].trim()
    } else {
        if tokens.len() != 1 {
            return None;
        }
        inline_unit
    };

    match unit.to_ascii_lowercase().as_str() {
        "m" | "min" | "mins" | "minute" | "minutes" | "分钟" | "分钟内" | "分" | "分内" => {
            Some(amount)
        }
        "h" | "hr" | "hrs" | "hour" | "hours" | "小时" | "小时内" | "时" | "时内" => {
            Some(amount * 60)
        }
        "d" | "day" | "days" | "天" | "天内" | "日" | "日内" => Some(amount * 24 * 60),
        _ => None,
    }
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
        let mut watcher = WxdbCommandWatcher {
            receiver: Some(receiver),
            enabled: true,
            stop: None,
            thread: None,
            state_path: None,
        };
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
        assert_eq!(
            backlog
                .requests
                .iter()
                .map(|request| request.room_id.as_str())
                .collect::<Vec<_>>(),
            vec!["room-a", "room-b"]
        );
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
        let mut backlog = ScheduledSummaryBacklog {
            requests: VecDeque::from([request]),
            ..Default::default()
        };
        let request = backlog.requests.pop_front().unwrap();

        requeue_scheduled_request_after_state_read_failure(&mut backlog, request);

        assert_eq!(backlog.requests.front(), Some(&expected));
        assert!(backlog.next_retry_at.is_some());
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
        assert_eq!(scheduler.pending.len(), 0);
        assert!(scheduler.in_flight.contains("room-b"));
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
        assert!(!scheduler.in_flight.contains("room-error"));

        assert_eq!(
            scheduler.enqueue(
                "room-panic".to_string(),
                Box::pin(async { panic!("expected panic") }),
            ),
            ScheduleResult::Started
        );
        tokio::task::yield_now().await;
        scheduler.reap(&test_config());
        assert!(!scheduler.in_flight.contains("room-panic"));

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

        assert!(!PipelineOptions::manual(&config, "test-room", false).image_gen_enabled);
        assert!(PipelineOptions::manual(&config, "test-room", true).image_gen_enabled);
    }

    #[test]
    fn manual_pipeline_image_argument_disables_when_default_on() {
        let mut config = test_config();
        config.image_gen.enabled = true;
        config.manual_summary.image_by_default = true;

        assert!(PipelineOptions::manual(&config, "test-room", false).image_gen_enabled);
        assert!(!PipelineOptions::manual(&config, "test-room", true).image_gen_enabled);
    }

    #[test]
    fn manual_pipeline_logs_retry_attempts() {
        let config = test_config();

        assert!(PipelineOptions::manual(&config, "test-room", false).log_retry_attempts);
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

        assert!(!PipelineOptions::manual(&config, "text-only-room", false).image_gen_enabled);
        assert!(PipelineOptions::manual(&config, "other-room", false).image_gen_enabled);
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
