//! Summary pipeline orchestration and platform-history preparation.

use crate::*;

#[derive(Debug, Clone, Copy)]
pub(crate) struct PipelineOptions {
    pub(crate) text_summary_enabled: bool,
    pub(crate) image_gen_enabled: bool,
    pub(crate) send_progress: bool,
    pub(crate) defer_text_until_image_ready: bool,
    pub(crate) send_disabled_message: bool,
    pub(crate) log_retry_attempts: bool,
    pub(crate) preview_only: bool,
    pub(crate) detail: wechat_summary_core::config::SummaryDetail,
    pub(crate) media_decode_limit: Option<usize>,
}

#[derive(Clone)]
pub(crate) struct ImageCooldownRecorder {
    pub(crate) store: SqliteStateStore,
    pub(crate) timestamp: DateTime<Utc>,
}

pub(crate) struct SummaryPipelineRequest<'a> {
    pub(crate) config: &'a AgentConfig,
    pub(crate) client: &'a PlatformWorker,
    pub(crate) incoming: &'a IncomingMessage,
    pub(crate) trigger: &'a TriggerMatch,
    pub(crate) range: &'a ResolvedTimeRange,
    pub(crate) options: PipelineOptions,
    pub(crate) image_pipeline_slots: &'a ImagePipelineSlotPool,
    pub(crate) image_cooldown_recorder: Option<ImageCooldownRecorder>,
    pub(crate) recent_observed_messages: Option<&'a RecentObservedMessages>,
    pub(crate) task: Option<&'a OperationalTask>,
}

pub(crate) struct PreparedPipelineInput {
    pub(crate) chat_messages: Vec<ChatMessage>,
    pub(crate) privacy: PrivacyFilter,
    pub(crate) llm_input: String,
    pub(crate) total_messages: usize,
    pub(crate) media: media_service::MediaEnrichment,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum PipelineOutcome {
    SummaryProduced,
    NoSummary,
}

impl PipelineOptions {
    pub(crate) fn manual(
        config: &AgentConfig,
        room_id: &str,
        image_token_present: bool,
        preview_only: bool,
    ) -> Self {
        let image_enabled_for_request =
            config.manual_summary.image_by_default ^ image_token_present;
        Self {
            text_summary_enabled: config.text_summary.enabled,
            image_gen_enabled: !preview_only
                && config.image_gen.enabled
                && config.image_summary_enabled_for_room(room_id)
                && image_enabled_for_request,
            send_progress: !preview_only,
            defer_text_until_image_ready: false,
            send_disabled_message: true,
            log_retry_attempts: true,
            preview_only,
            detail: config
                .room_policy(room_id)
                .summary_detail
                .unwrap_or(config.text_summary.detail),
            media_decode_limit: None,
        }
    }

    pub(crate) fn scheduled(config: &AgentConfig, room_id: &str) -> Self {
        Self {
            text_summary_enabled: config.scheduled_summary.send_text && config.text_summary.enabled,
            image_gen_enabled: config.scheduled_summary.send_image
                && config.image_gen.enabled
                && config.image_summary_enabled_for_room(room_id),
            send_progress: false,
            defer_text_until_image_ready: true,
            send_disabled_message: false,
            log_retry_attempts: false,
            preview_only: false,
            detail: config
                .room_policy(room_id)
                .summary_detail
                .unwrap_or(config.text_summary.detail),
            media_decode_limit: None,
        }
    }
}

pub(crate) fn image_pipeline_slot_capacity(
    config: &AgentConfig,
    platform_rooms: &[String],
) -> usize {
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
pub(crate) async fn query_platform_history_paginated(
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

pub(crate) fn platform_history_message_key(message: &PlatformHistoryMessage) -> String {
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

pub(crate) fn platform_history_order(
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

pub(crate) fn stable_id_number(value: &str) -> Option<u64> {
    value
        .rsplit_once(':')
        .map(|(_, id)| id)
        .unwrap_or(value)
        .parse()
        .ok()
}

pub(crate) fn stable_ids_match(left: &str, right: &str) -> bool {
    left == right
        || stable_id_number(left).is_some() && stable_id_number(left) == stable_id_number(right)
}

pub(crate) fn media_decode_attempt_count(messages: &[PlatformHistoryMessage]) -> usize {
    messages
        .iter()
        .filter(|message| {
            message.decoded_media_path.is_some() || message.media_decode_error.is_some()
        })
        .count()
}

pub(crate) async fn run_summary_pipeline(
    request: SummaryPipelineRequest<'_>,
) -> Result<PipelineOutcome> {
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

    let Some(mut history) = pipeline_input::load_summary_history(
        config,
        client,
        incoming,
        trigger,
        range,
        recent_observed_messages,
        options.media_decode_limit,
    )
    .await?
    else {
        return Ok(PipelineOutcome::NoSummary);
    };
    let platform_history_len = history.len();
    if let Some(task) = task {
        task.set_stage(
            TaskState::Running,
            "history",
            None,
            None,
            platform_history_len as u64,
            0,
        );
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
        !history_rules::is_current_trigger(message, incoming)
            && !history_rules::is_agent_status(message)
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

    let Some(prepared_input) = pipeline_input::prepare_pipeline_input(
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
    let (llm_config, circuit_bypassed) = routed_llm_config(config, task.map(|task| &task.store));
    if circuit_bypassed {
        info!(room_id = %trigger.room_id, provider = %llm_config.provider, "primary LLM circuit is open; using configured fallback");
        append_runtime_log(
            config,
            &format!(
                "llm primary circuit open room={} fallback_provider={}",
                trigger.room_id, llm_config.provider
            ),
        );
    }
    let mut llm = configure_llm_tracing(
        OpenAiCompatibleLlm::new(llm_config.clone(), &config.proxy)
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
            options.detail,
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
        if options.preview_only {
            if let Some(task) = task {
                task.set_stage(
                    TaskState::Succeeded,
                    "preview_ready",
                    Some(&summary),
                    None,
                    chat_messages.len() as u64,
                    media.total() as u64,
                );
            }
            append_runtime_log(
                config,
                &format!("summary preview ready room={}", trigger.room_id),
            );
            return Ok(PipelineOutcome::SummaryProduced);
        }
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
            client.clone(),
            trigger.room_id.clone(),
            llm_input,
            chat_messages,
            options.text_summary_enabled,
            image_pipeline_slots.clone(),
            image_cooldown_recorder,
            llm_config,
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
                    deliver_outboxed_image(config, task, client, &trigger.room_id, &artifact)
                        .await?;
                } else {
                    summary_image::send_with_worker(config, client, &trigger.room_id, &artifact)
                        .await?;
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
