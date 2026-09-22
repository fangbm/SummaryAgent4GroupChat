//! Lifecycle management for the wxdb recovered-command watcher thread.

use std::{
    collections::{HashMap, VecDeque},
    fs,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering as AtomicOrdering},
        mpsc, Arc,
    },
    thread,
    time::Duration as StdDuration,
};

use crate::{
    platform::PlatformEvent,
    runtime_log::{append_runtime_log, compact_error_for_runtime},
};
use anyhow::{Context, Result};
use chrono::{DateTime, Duration, TimeZone, Utc};
use serde::{Deserialize, Serialize};
use wechat_summary_core::{
    config::PlatformKindConfig, models::IncomingMessage, AgentConfig, TriggerMatcher,
};

pub(crate) struct WxdbCommandWatcher {
    receiver: Option<mpsc::Receiver<PlatformEvent>>,
    enabled: bool,
    stop: Option<Arc<AtomicBool>>,
    thread: Option<thread::JoinHandle<()>>,
    state_path: Option<PathBuf>,
}

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub(crate) enum WxdbCommandWatcherRecv {
    Event(PlatformEvent),
    Empty,
    Disconnected,
}

impl WxdbCommandWatcher {
    pub(crate) fn stopped() -> Self {
        Self {
            receiver: None,
            enabled: false,
            stop: None,
            thread: None,
            state_path: None,
        }
    }

    pub(crate) fn start(config: &AgentConfig) -> Self {
        Self::start_with_state_path(config, None)
    }

    pub(crate) fn start_with_state_path(
        config: &AgentConfig,
        previous_state_path: Option<PathBuf>,
    ) -> Self {
        if !enabled(config) {
            return Self::stopped();
        }

        let state_path = previous_state_path.unwrap_or_else(|| state_path(config));
        let rooms = configured_rooms(config);
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
        let thread =
            thread::spawn(move || run(config, rooms, sender, thread_stop, thread_state_path));
        Self {
            receiver: Some(receiver),
            enabled: true,
            stop: Some(stop),
            thread: Some(thread),
            state_path: Some(state_path),
        }
    }

    pub(crate) fn try_recv(&mut self) -> WxdbCommandWatcherRecv {
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

    pub(crate) fn enabled(&self) -> bool {
        self.enabled
    }

    pub(crate) fn state_path(&self) -> Option<&Path> {
        self.state_path.as_deref()
    }

    #[cfg(test)]
    pub(crate) fn with_receiver_for_test(receiver: mpsc::Receiver<PlatformEvent>) -> Self {
        Self {
            receiver: Some(receiver),
            enabled: true,
            stop: None,
            thread: None,
            state_path: None,
        }
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

pub(crate) fn enabled(config: &AgentConfig) -> bool {
    config
        .platform
        .enabled_kinds()
        .contains(&PlatformKindConfig::Wx4py)
        && !config.wx_cli.executable.trim().is_empty()
}

pub(crate) fn configured_rooms(config: &AgentConfig) -> Vec<String> {
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

fn state_path(config: &AgentConfig) -> PathBuf {
    Path::new(&config.runtime.output_dir).join("wxdb-command-watcher-state.json")
}

fn run(
    config: AgentConfig,
    rooms: Vec<String>,
    sender: mpsc::Sender<PlatformEvent>,
    stop: Arc<AtomicBool>,
    watermark_path: PathBuf,
) {
    let matcher = match TriggerMatcher::new(crate::effective_listen_config(&config)) {
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
            crate::WXDB_COMMAND_WATCH_INTERVAL_SECONDS,
            crate::WXDB_COMMAND_WATCH_LOOKBACK_SECONDS,
            effective_cache_dir(&config)
        ),
    );

    let mut watcher_state = load_state(&watermark_path);
    let startup_now = Utc::now().timestamp();
    for room in &rooms {
        watcher_state
            .rooms
            .entry(room.clone())
            .or_insert_with(|| RoomState::new(startup_now));
    }
    save_state(&watermark_path, &watcher_state);
    let mut recent_errors = WatcherErrors::default();
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
                .or_insert_with(|| RoomState::new(now.timestamp()));
            let since_timestamp = state
                .cursor_timestamp
                .saturating_sub(crate::WXDB_COMMAND_WATCH_LOOKBACK_SECONDS);
            let since = Utc
                .timestamp_opt(since_timestamp, 0)
                .single()
                .unwrap_or(now);
            match poll_room(&config, &matcher, room, since, now, state) {
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
                    save_state(&watermark_path, &watcher_state);
                }
                Err(error) => recent_errors.record(&config, room, &error),
            }
        }
        for _ in 0..crate::WXDB_COMMAND_WATCH_INTERVAL_SECONDS {
            if stop.load(AtomicOrdering::Acquire) {
                return;
            }
            thread::sleep(StdDuration::from_secs(1));
        }
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct WatcherState {
    rooms: HashMap<String, RoomState>,
}

#[derive(Debug, Serialize, Deserialize)]
struct RoomState {
    startup_baseline: i64,
    cursor_timestamp: i64,
    #[serde(default)]
    initialized: bool,
    #[serde(default)]
    last_seen_local_id: Option<i64>,
    #[serde(default)]
    seen_ids: VecDeque<String>,
}

impl RoomState {
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
        while self.seen_ids.len() > crate::WXDB_COMMAND_WATCH_SEEN_IDS {
            self.seen_ids.pop_front();
        }
    }
}

fn load_state(path: &Path) -> WatcherState {
    fs::read_to_string(path)
        .ok()
        .and_then(|text| serde_json::from_str(&text).ok())
        .unwrap_or_default()
}

fn save_state(path: &Path, state: &WatcherState) {
    if let Some(parent) = path.parent() {
        let _ = fs::create_dir_all(parent);
    }
    if let Ok(text) = serde_json::to_string(state) {
        let _ = fs::write(path, text);
    }
}

#[derive(Default)]
struct WatcherErrors {
    by_room: HashMap<String, WatcherErrorState>,
}

struct WatcherErrorState {
    first_seen: DateTime<Utc>,
    last_logged: DateTime<Utc>,
    suppressed: usize,
}

impl WatcherErrors {
    fn record(&mut self, config: &AgentConfig, room: &str, error: &anyhow::Error) {
        let now = Utc::now();
        let state = self
            .by_room
            .entry(room.to_string())
            .or_insert(WatcherErrorState {
                first_seen: now,
                last_logged: now
                    - Duration::seconds(crate::WXDB_COMMAND_WATCH_ERROR_LOG_INTERVAL_SECONDS),
                suppressed: 0,
            });

        if now.signed_duration_since(state.last_logged).num_seconds()
            >= crate::WXDB_COMMAND_WATCH_ERROR_LOG_INTERVAL_SECONDS
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

fn poll_room(
    config: &AgentConfig,
    matcher: &TriggerMatcher,
    room: &str,
    since: DateTime<Utc>,
    until: DateTime<Utc>,
    state: &mut RoomState,
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
            crate::WXDB_COMMAND_WATCH_LIMIT as u32,
            Some(0),
            None,
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

    let messages = crate::paginate_wxdb_history(
        crate::WXDB_COMMAND_WATCH_LIMIT,
        state.last_seen_local_id,
        query_page,
    )?;
    let max_local_id = messages.iter().filter_map(|message| message.local_id).max();
    let mut events = Vec::new();
    for message in messages {
        let Some(key) = seen_message_key(chat_name, &message) else {
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
        if content.is_empty() || crate::history_rules::is_agent_status_content(&content) {
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
        // wxdb is the recovery path for messages which the UIAutomation
        // listener misses while the WeChat window is unavailable. Keep the
        // built-in image commands on that path too, rather than recovering
        // only configurable summary triggers.
        if !is_recoverable_command(matcher, &incoming) {
            continue;
        }
        state.remember(key);
        events.push(PlatformEvent {
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
            task_id_hint: None,
        });
    }
    state.last_seen_local_id = max_local_id.or(state.last_seen_local_id);

    events.sort_by(|left, right| {
        left.timestamp.cmp(&right.timestamp).then_with(|| {
            crate::stable_id_number(left.stable_id.as_deref().unwrap_or("")).cmp(
                &crate::stable_id_number(right.stable_id.as_deref().unwrap_or("")),
            )
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

fn is_recoverable_command(matcher: &TriggerMatcher, incoming: &IncomingMessage) -> bool {
    matcher.match_message(incoming).is_some()
        || (matcher.allows_message(incoming)
            && crate::parse_image_command(&incoming.content).is_some())
}

fn effective_cache_dir(config: &AgentConfig) -> String {
    let cache_dir = config.wx_cli.cache_dir.trim();
    if cache_dir.is_empty() {
        "<default>".to_string()
    } else {
        cache_dir.to_string()
    }
}

fn seen_message_key(
    chat_name: &str,
    message: &wx4py_client::Wx4pyHistoryMessage,
) -> Option<String> {
    message
        .local_id
        .map(|local_id| format!("{chat_name}:local:{local_id}"))
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use wechat_summary_core::config::{ListenConfig, MatchMode};

    use super::*;

    fn matcher() -> TriggerMatcher {
        TriggerMatcher::new(ListenConfig {
            triggers: vec!["/总结".to_string()],
            match_mode: MatchMode::Prefix,
            whitelist_rooms: vec!["room".to_string()],
            blacklist_users: Vec::new(),
            content_types: vec!["text".to_string()],
            ignore_self: true,
            require_allowed_users: false,
            allowed_users: Vec::new(),
        })
        .expect("valid matcher")
    }

    fn incoming(content: &str) -> IncomingMessage {
        IncomingMessage {
            room_id: "room".to_string(),
            room_name: Some("room".to_string()),
            stable_id: Some("local:1".to_string()),
            sender_id: "sender".to_string(),
            sender_name: None,
            content: content.to_string(),
            msg_type: "text".to_string(),
            timestamp: Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
            is_self: false,
        }
    }

    #[test]
    fn recovers_builtin_image_commands_when_realtime_listener_misses_them() {
        let matcher = matcher();
        assert!(is_recoverable_command(&matcher, &incoming("/图片")));
        assert!(is_recoverable_command(&matcher, &incoming("/image city at night")));
    }

    #[test]
    fn does_not_recover_arbitrary_room_text() {
        assert!(!is_recoverable_command(&matcher(), &incoming("普通聊天内容")));
    }
}
