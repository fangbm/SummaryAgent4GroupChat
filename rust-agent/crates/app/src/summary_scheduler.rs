//! Concurrent summary task scheduling with per-room de-duplication.

use std::{
    collections::{HashSet, VecDeque},
    future::Future,
    pin::Pin,
    sync::mpsc,
};

use anyhow::Result;
use tokio::task::JoinSet;
use tracing::{error, warn};
use wechat_summary_core::AgentConfig;

use crate::{append_runtime_log, format_error_chain};

pub(crate) type SummaryFuture = Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>>;

struct PendingSummaryTask {
    room_id: String,
    future: SummaryFuture,
}

pub(crate) struct SummaryTaskScheduler {
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

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum ScheduleResult {
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
    pub(crate) fn new(max_concurrency: usize, pending_capacity: usize) -> Self {
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

    pub(crate) fn enqueue(
        &mut self,
        room_id: String,
        future: SummaryFuture,
    ) -> ScheduleResult {
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

    pub(crate) fn reap(&mut self, config: &AgentConfig) {
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

    #[cfg(test)]
    pub(crate) fn pending_len(&self) -> usize {
        self.pending.len()
    }

    #[cfg(test)]
    pub(crate) fn is_in_flight(&self, room_id: &str) -> bool {
        self.in_flight.contains(room_id)
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
