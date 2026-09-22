//! Scheduled-summary backlog with bounded exponential retry timing.

use std::{
    collections::VecDeque,
    time::{Duration, Instant},
};

use chrono::{DateTime, Utc};
use wechat_summary_core::ResolvedTimeRange;

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct ScheduledSummaryRequest {
    pub(crate) room_id: String,
    pub(crate) range: ResolvedTimeRange,
    pub(crate) due_at: DateTime<Utc>,
}

#[derive(Default)]
pub(crate) struct ScheduledSummaryBacklog {
    requests: VecDeque<ScheduledSummaryRequest>,
    next_retry_at: Option<Instant>,
    retry_delay: Duration,
}

impl ScheduledSummaryBacklog {
    pub(crate) fn add_rooms(
        &mut self,
        rooms: impl IntoIterator<Item = String>,
        range: ResolvedTimeRange,
        due_at: DateTime<Utc>,
    ) {
        for room_id in rooms {
            if self
                .requests
                .iter()
                .any(|request| request.room_id == room_id)
            {
                continue;
            }
            self.requests.push_back(ScheduledSummaryRequest {
                room_id,
                range: range.clone(),
                due_at,
            });
        }
    }

    pub(crate) fn clear(&mut self) {
        self.requests.clear();
        self.clear_retry();
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.requests.is_empty()
    }

    pub(crate) fn has_pending(&self) -> bool {
        !self.is_empty()
    }

    pub(crate) fn pop_next(&mut self) -> Option<ScheduledSummaryRequest> {
        self.requests.pop_front()
    }

    pub(crate) fn requeue_front(&mut self, request: ScheduledSummaryRequest) {
        self.requests.push_front(request);
    }

    pub(crate) fn requeue_back(&mut self, request: ScheduledSummaryRequest) {
        self.requests.push_back(request);
    }

    pub(crate) fn retry_ready(&self, now: Instant) -> bool {
        self.next_retry_at.is_none_or(|retry_at| now >= retry_at)
    }

    pub(crate) fn record_retry(&mut self, now: Instant) {
        let delay = if self.retry_delay.is_zero() {
            Duration::from_secs(1)
        } else {
            self.retry_delay.min(Duration::from_secs(30))
        };
        self.next_retry_at = Some(now + delay);
        self.retry_delay = (delay * 2).min(Duration::from_secs(30));
    }

    pub(crate) fn clear_retry(&mut self) {
        self.next_retry_at = None;
        self.retry_delay = Duration::ZERO;
    }

    #[cfg(test)]
    pub(crate) fn room_ids(&self) -> Vec<&str> {
        self.requests
            .iter()
            .map(|request| request.room_id.as_str())
            .collect()
    }

    #[cfg(test)]
    pub(crate) fn front(&self) -> Option<&ScheduledSummaryRequest> {
        self.requests.front()
    }

    #[cfg(test)]
    pub(crate) fn has_retry_scheduled(&self) -> bool {
        self.next_retry_at.is_some()
    }
}
