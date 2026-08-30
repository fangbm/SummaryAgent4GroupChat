//! Scheduled-summary and task-center retry execution.

use crate::*;

pub(crate) fn drain_scheduled_backlog(
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
    while let Some(request) = backlog.pop_next() {
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
                    requeue_after_state_read_failure(backlog, request);
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
                backlog.requeue_back(request);
                append_runtime_log(
                    config,
                    &format!("scheduled summary pending room={room} reason=in_flight"),
                );
            }
            ScheduleResult::QueueFull => {
                queue_full = true;
                backlog.requeue_back(request);
                append_runtime_log(
                    config,
                    &format!("scheduled summary pending room={room} reason=queue_full"),
                );
            }
        }
    }
    if queue_full || backlog.has_pending() {
        backlog.record_retry(Instant::now());
    } else {
        backlog.clear_retry();
    }
}

#[cfg(test)]
pub(crate) fn requeue_scheduled_request_after_state_read_failure(
    backlog: &mut ScheduledSummaryBacklog,
    request: ScheduledSummaryRequest,
) {
    requeue_after_state_read_failure(backlog, request);
}

fn requeue_after_state_read_failure(
    backlog: &mut ScheduledSummaryBacklog,
    request: ScheduledSummaryRequest,
) {
    backlog.requeue_front(request);
    backlog.record_retry(Instant::now());
}

pub(crate) fn drain_manual_retry_tasks(
    config: &AgentConfig,
    store: &SqliteStateStore,
    client: &PlatformWorker,
    image_pipeline_slots: &ImagePipelineSlotPool,
    scheduler: &mut SummaryTaskScheduler,
) {
    let retries = match store.queued_retry_tasks(8) {
        Ok(tasks) => tasks,
        Err(error) => {
            append_runtime_log(
                config,
                &format!("manual retry task query failed error={error}"),
            );
            return;
        }
    };
    for record in retries {
        let room_id = record.room_id.clone();
        let scheduler_room_id = room_id.clone();
        let task = OperationalTask {
            id: record.id.clone(),
            store: store.clone(),
        };
        let task_config = config.clone();
        let task_client = client.clone();
        let task_slots = image_pipeline_slots.clone();
        let task_for_future = task.clone();
        let future = Box::pin(async move {
            task_for_future.set_stage(TaskState::Running, "retry_accepted", None, None, 0, 0);
            let incoming = IncomingMessage {
                room_id: room_id.clone(),
                room_name: Some(room_id.clone()),
                stable_id: None,
                sender_id: "task_center".to_string(),
                sender_name: Some("任务中心重试".to_string()),
                content: "[task_retry]".to_string(),
                msg_type: "text".to_string(),
                timestamp: Utc::now(),
                is_self: true,
            };
            let trigger = TriggerMatch {
                room_id: room_id.clone(),
                trigger_symbol: "[task_retry]".to_string(),
                trigger_content: "[task_retry]".to_string(),
            };
            let range = ResolvedTimeRange {
                since: record.since,
                until: record.until,
                mode: TimeRangeMode::FixedMinutes,
            };
            let result = run_summary_pipeline(SummaryPipelineRequest {
                config: &task_config,
                client: &task_client,
                incoming: &incoming,
                trigger: &trigger,
                range: &range,
                options: PipelineOptions::manual(&task_config, &room_id, false, false),
                image_pipeline_slots: &task_slots,
                image_cooldown_recorder: Some(ImageCooldownRecorder {
                    store: task_for_future.store.clone(),
                    timestamp: Utc::now(),
                }),
                recent_observed_messages: None,
                task: Some(&task_for_future),
            })
            .await;
            match &result {
                Ok(PipelineOutcome::SummaryProduced) => task_for_future.set_stage(
                    TaskState::Succeeded,
                    "retry_completed",
                    None,
                    None,
                    0,
                    0,
                ),
                Ok(PipelineOutcome::NoSummary) => task_for_future.set_stage(
                    TaskState::Succeeded,
                    "retry_completed_without_output",
                    None,
                    None,
                    0,
                    0,
                ),
                Err(error) => task_for_future.set_stage(
                    TaskState::Failed,
                    "retry_failed",
                    None,
                    Some(&format_error_chain(error)),
                    0,
                    0,
                ),
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
    let mut options = PipelineOptions::scheduled(config, &trigger.room_id);
    match daily_budget_state(
        config,
        store,
        &trigger.room_id,
        now,
        options.image_gen_enabled,
    ) {
        Ok(budget) => options.media_decode_limit = budget.media_decode_limit,
        Err(error) => {
            append_runtime_log(
                config,
                &format!(
                    "scheduled summary skipped room={} reason=budget error={error}",
                    trigger.room_id
                ),
            );
            return Ok(());
        }
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
        options,
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
            task.set_stage(
                TaskState::Succeeded,
                "completed_without_output",
                None,
                None,
                0,
                0,
            );
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
            task.set_stage(
                TaskState::Failed,
                "failed",
                None,
                Some(&error_message),
                0,
                0,
            );
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
