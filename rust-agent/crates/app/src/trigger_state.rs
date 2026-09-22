//! In-memory trigger de-duplication and recent realtime-message tracking.

use std::collections::{HashMap, VecDeque};

use chrono::{DateTime, Duration, Utc};
use wechat_summary_core::{models::IncomingMessage, TriggerMatch};

#[derive(Debug, Default)]
pub(crate) struct RecentTriggerAttempts {
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
    pub(crate) fn is_duplicate_at(
        &mut self,
        trigger: &TriggerMatch,
        event_at: DateTime<Utc>,
        observed_at: DateTime<Utc>,
    ) -> bool {
        self.is_duplicate_at_with_id_inner(trigger, None, event_at, observed_at)
    }

    pub(crate) fn is_duplicate_with_id(
        &mut self,
        trigger: &TriggerMatch,
        stable_id: Option<&str>,
        event_at: DateTime<Utc>,
    ) -> bool {
        self.is_duplicate_at_with_id_inner(trigger, stable_id, event_at, Utc::now())
    }

    #[cfg(test)]
    pub(crate) fn is_duplicate_at_with_id(
        &mut self,
        trigger: &TriggerMatch,
        stable_id: Option<&str>,
        event_at: DateTime<Utc>,
        observed_at: DateTime<Utc>,
    ) -> bool {
        self.is_duplicate_at_with_id_inner(trigger, stable_id, event_at, observed_at)
    }

    fn is_duplicate_at_with_id_inner(
        &mut self,
        trigger: &TriggerMatch,
        stable_id: Option<&str>,
        event_at: DateTime<Utc>,
        observed_at: DateTime<Utc>,
    ) -> bool {
        let retention_cutoff =
            observed_at - Duration::seconds(crate::TRIGGER_DEDUPE_RETENTION_SECONDS);
        self.attempts_by_key.retain(|_, attempts| {
            attempts.retain(|attempt| attempt.observed_at >= retention_cutoff);
            !attempts.is_empty()
        });
        let process_cutoff = observed_at - Duration::seconds(crate::TRIGGER_DEDUPE_WINDOW_SECONDS);
        let key = trigger_key(trigger);
        if self.attempts_by_key.get(&key).is_some_and(|attempts| {
            attempts
                .iter()
                .any(|attempt| match (stable_id, attempt.stable_id.as_deref()) {
                    (Some(current), Some(previous)) => crate::stable_ids_match(previous, current),
                    _ => {
                        attempt.observed_at >= process_cutoff
                            || event_times_close(attempt.event_at, event_at)
                    }
                })
        }) {
            return true;
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

#[derive(Debug, Clone, Default)]
pub(crate) struct RecentObservedMessages {
    messages: VecDeque<IncomingMessage>,
}

impl RecentObservedMessages {
    pub(crate) fn record(&mut self, message: &IncomingMessage, now: DateTime<Utc>) {
        if message.msg_type != "text" || message.content.trim().is_empty() {
            return;
        }
        self.messages.push_back(message.clone());
        self.prune(now);
    }
    pub(crate) fn count_user_text_in_range(
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
                    && !crate::history_rules::is_current_incoming(message, incoming)
                    && !crate::history_rules::is_agent_status_content(&message.content)
            })
            .count()
    }
    pub(crate) fn has_matching_trigger(
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
                .is_some_and(|(left, right)| crate::stable_ids_match(left, right));
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
        let cutoff = now - Duration::hours(crate::RECENT_OBSERVED_WINDOW_HOURS);
        self.messages.retain(|message| message.timestamp >= cutoff);
        while self.messages.len() > crate::RECENT_OBSERVED_MAX_MESSAGES {
            self.messages.pop_front();
        }
    }
}

fn event_times_close(left: DateTime<Utc>, right: DateTime<Utc>) -> bool {
    let delta = (left - right).num_seconds();
    (-crate::TRIGGER_DEDUPE_EVENT_WINDOW_SECONDS..=crate::TRIGGER_DEDUPE_EVENT_WINDOW_SECONDS)
        .contains(&delta)
}

fn trigger_key(trigger: &TriggerMatch) -> String {
    format!(
        "{}\n{}",
        trigger.room_id.trim(),
        trigger.trigger_content.trim()
    )
}
