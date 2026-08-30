//! Shared task-center state updates for manual, scheduled, and retry flows.

use tracing::warn;
use wechat_summary_storage::{SqliteStateStore, TaskState};

#[derive(Clone)]
pub(crate) struct OperationalTask {
    pub(crate) id: String,
    pub(crate) store: SqliteStateStore,
}

impl OperationalTask {
    pub(crate) fn set_stage(
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

    pub(crate) fn cancelled(&self) -> bool {
        self.store
            .task(&self.id)
            .ok()
            .flatten()
            .is_some_and(|task| task.state == TaskState::Cancelled)
    }
}
