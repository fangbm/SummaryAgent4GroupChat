//! Lifecycle management for the wxdb recovered-command watcher thread.

use std::{
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering as AtomicOrdering},
        mpsc, Arc,
    },
    thread,
};

use wechat_summary_core::{config::PlatformKindConfig, AgentConfig};

use crate::{append_runtime_log, platform::PlatformEvent};

pub(crate) struct WxdbCommandWatcher {
    receiver: Option<mpsc::Receiver<PlatformEvent>>,
    enabled: bool,
    stop: Option<Arc<AtomicBool>>,
    thread: Option<thread::JoinHandle<()>>,
    state_path: Option<PathBuf>,
}

#[derive(Debug)]
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
        let thread = thread::spawn(move || {
            crate::run_wxdb_command_watcher(config, rooms, sender, thread_stop, thread_state_path)
        });
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
    config.platform.kind == PlatformKindConfig::Wx4py && !config.wx_cli.executable.trim().is_empty()
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
