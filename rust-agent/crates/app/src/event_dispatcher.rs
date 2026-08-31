//! Platform events to summary-task dispatch.

use crate::*;

#[allow(clippy::too_many_arguments)]
pub(crate) fn enqueue_platform_event(
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
    if matcher.match_message(&incoming).is_none()
        && (!matcher.allows_message(&incoming)
            || crate::parse_image_command(&incoming.content).is_none())
    {
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

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum PlatformEventSource {
    Realtime,
    WxdbRecovered,
}
