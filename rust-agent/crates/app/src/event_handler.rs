//! Manual platform-trigger handling and pipeline result bookkeeping.

use crate::*;

#[allow(clippy::too_many_arguments)]
pub(crate) async fn handle_platform_event(
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
    if matcher.allows_message(&incoming) {
        if let Some(command) = parse_image_command(&incoming.content) {
            return handle_manual_image_command(
                config,
                store,
                client,
                recent_trigger_attempts,
                image_pipeline_slots,
                source_platform,
                event_source,
                &incoming,
                command,
            )
            .await;
        }
    }
    let Some(trigger) = matcher.match_message(&incoming) else {
        return Ok(());
    };
    if !config.trigger_user_allowed(&trigger.room_id, &incoming.sender_id) {
        info!(room_id = %trigger.room_id, "trigger ignored because sender is not in the allowed-user policy");
        append_runtime_log(
            config,
            &format!(
                "trigger ignored room={} reason=sender_not_allowed",
                trigger.room_id
            ),
        );
        return Ok(());
    }
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
            "暂不支持跨平台总结：当前指令来自 {}，目标为 {}。请在目标平台对应的群聊或频道内发送指令。",
            source_platform.as_str(),
            command.target_platform.as_str()
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

    let mut pipeline_options = PipelineOptions::manual(
        config,
        &trigger.room_id,
        command.image_token_present,
        command.preview_only,
    );
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

    match daily_budget_state(
        config,
        store,
        &trigger.room_id,
        incoming.timestamp,
        pipeline_options.image_gen_enabled,
    ) {
        Ok(budget) => pipeline_options.media_decode_limit = budget.media_decode_limit,
        Err(error) => {
            let message = format!("本群今日总结额度限制：{error}");
            append_runtime_log(
                config,
                &format!(
                    "trigger rejected room={} reason=budget error={error}",
                    trigger.room_id
                ),
            );
            let _ = client.send_text(&trigger.room_id, &message).await;
            return Ok(());
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
        preview_only = command.preview_only,
        "trigger accepted; running summary pipeline"
    );
    append_runtime_log(
        config,
        &format!(
            "trigger accepted room={} source_platform={} target_platform={} since={} until={} command_range_minutes={:?} image_token_present={} preview_only={}",
            trigger.room_id,
            source_platform.as_str(),
            command.target_platform.as_str(),
            range.since,
            range.until,
            command.range_minutes,
            command.image_token_present
            ,command.preview_only
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
            task.set_stage(
                TaskState::Succeeded,
                "completed_without_output",
                None,
                None,
                0,
                0,
            );
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
            task.set_stage(
                TaskState::Failed,
                "failed",
                None,
                Some(&error_message),
                0,
                0,
            );
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

#[allow(clippy::too_many_arguments)]
async fn handle_manual_image_command(
    config: &AgentConfig,
    store: &SqliteStateStore,
    client: &PlatformWorker,
    recent_trigger_attempts: &Arc<Mutex<RecentTriggerAttempts>>,
    image_pipeline_slots: &ImagePipelineSlotPool,
    source_platform: PlatformKindConfig,
    event_source: PlatformEventSource,
    incoming: &IncomingMessage,
    command: crate::image_command::ImageCommand,
) -> Result<()> {
    let trigger = TriggerMatch {
        room_id: incoming.room_id.clone(),
        trigger_symbol: command.trigger_symbol,
        trigger_content: incoming.content.clone(),
    };
    if !config.trigger_user_allowed(&trigger.room_id, &incoming.sender_id) {
        append_runtime_log(
            config,
            &format!(
                "manual image command ignored room={} reason=sender_not_allowed",
                trigger.room_id
            ),
        );
        return Ok(());
    }
    if recent_trigger_attempts
        .lock()
        .map(|mut attempts| {
            attempts.is_duplicate_with_id(
                &trigger,
                incoming.stable_id.as_deref(),
                incoming.timestamp,
            )
        })
        .unwrap_or(false)
    {
        append_runtime_log(
            config,
            &format!(
                "manual image command ignored room={} reason=duplicate",
                trigger.room_id
            ),
        );
        return Ok(());
    }
    if !config.novelai.enabled {
        let _ = client
            .send_text(&trigger.room_id, "NovelAI 图片命令未启用。")
            .await;
        return Ok(());
    }

    let task = OperationalTask {
        id: store
            .create_task(NewTask {
                room_id: &trigger.room_id,
                source: match (event_source, command.prompt.is_some()) {
                    (PlatformEventSource::Realtime, true) => "manual_image",
                    (PlatformEventSource::WxdbRecovered, true) => "manual_image_wxdb_recovered",
                    (PlatformEventSource::Realtime, false) => "manual_image_random",
                    (PlatformEventSource::WxdbRecovered, false) => {
                        "manual_image_random_wxdb_recovered"
                    }
                },
                since: incoming.timestamp,
                until: incoming.timestamp,
                config_revision: &config_revision(config),
                retry_of: None,
            })?
            .id,
        store: store.clone(),
    };
    let replay_generated_image = command.prompt.is_none();
    task.set_stage(
        TaskState::Running,
        if replay_generated_image {
            "manual_image_random"
        } else {
            "manual_image_prompt"
        },
        None,
        None,
        0,
        0,
    );
    info!(
        room_id = %trigger.room_id,
        source_platform = source_platform.as_str(),
        prompt_chars = command.prompt.as_deref().unwrap_or_default().chars().count(),
        replay_generated_image,
        "manual image command accepted"
    );
    if replay_generated_image {
        let artifact = match runtime_artifacts::random_generated_image(config) {
            Ok(Some(artifact)) => artifact,
            Ok(None) => {
                task.set_stage(
                    TaskState::Failed,
                    "no_generated_image",
                    None,
                    Some("no generated image artifacts are available"),
                    0,
                    0,
                );
                let _ = client
                    .send_text(&trigger.room_id, "暂无可随机发送的已生成图片。")
                    .await;
                return Ok(());
            }
            Err(error) => {
                let detail = format_error_chain(&error);
                task.set_stage(
                    TaskState::Failed,
                    "random_image_lookup_failed",
                    None,
                    Some(&detail),
                    0,
                    0,
                );
                let _ = client
                    .send_text(
                        &trigger.room_id,
                        &format_failure_message_for_chat("读取已生成图片失败", &detail),
                    )
                    .await;
                return Ok(());
            }
        };
        info!(
            room_id = %trigger.room_id,
            artifact_size_bytes = artifact.size_bytes,
            "manual random image delivery starting"
        );
        let _ = client
            .send_text(&trigger.room_id, "正在随机发送一张已生成图片...")
            .await;
        match outbox::deliver_image(config, &task, client, &trigger.room_id, &artifact).await {
            Ok(()) => {
                task.set_stage(TaskState::Succeeded, "completed", None, None, 0, 0);
                append_runtime_log(
                    config,
                    &format!(
                        "manual random image command completed room={}",
                        trigger.room_id
                    ),
                );
            }
            Err(error) => {
                let detail = format_error_chain(&error);
                error!(room_id = %trigger.room_id, error = %detail, "manual random image delivery failed");
                append_runtime_log(
                    config,
                    &format!(
                        "manual random image delivery failed room={} error={detail}",
                        trigger.room_id
                    ),
                );
                task.set_stage(
                    TaskState::Failed,
                    "delivery_failed",
                    None,
                    Some(&detail),
                    0,
                    0,
                );
                let _ = client
                    .send_text(
                        &trigger.room_id,
                        &format_failure_message_for_chat("图片发送失败", &detail),
                    )
                    .await;
            }
        }
        return Ok(());
    }

    let prompt = command
        .prompt
        .as_deref()
        .expect("prompt is present after replay branch");
    let _ = client.send_text(&trigger.room_id, "正在生成图片...").await;
    match summary_image::generate_manual_novelai_image(
        config,
        image_pipeline_slots,
        &trigger.room_id,
        prompt,
    )
    .await
    {
        Ok(artifact) => {
            match outbox::deliver_image(config, &task, client, &trigger.room_id, &artifact).await {
                Ok(()) => {
                    task.set_stage(TaskState::Succeeded, "completed", None, None, 0, 0);
                    append_runtime_log(
                        config,
                        &format!("manual image command completed room={}", trigger.room_id),
                    );
                }
                Err(error) => {
                    let detail = format_error_chain(&error);
                    task.set_stage(
                        TaskState::Failed,
                        "delivery_failed",
                        None,
                        Some(&detail),
                        0,
                        0,
                    );
                    let _ = client
                        .send_text(
                            &trigger.room_id,
                            &format_failure_message_for_chat("图片发送失败", &detail),
                        )
                        .await;
                }
            }
        }
        Err(error) => {
            let detail = format_error_chain(&error);
            task.set_stage(TaskState::Failed, "failed", None, Some(&detail), 0, 0);
            error!(room_id = %trigger.room_id, error = %detail, "manual image command failed");
            let _ = client
                .send_text(
                    &trigger.room_id,
                    &format_failure_message_for_chat("图片生成失败", &detail),
                )
                .await;
        }
    }
    Ok(())
}
