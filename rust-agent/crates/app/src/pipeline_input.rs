//! Summary-specific history recovery and LLM input preparation.

use std::time::Duration as StdDuration;

use anyhow::{Context, Result};
use tracing::{info, warn};
use wechat_summary_core::{models::IncomingMessage, AgentConfig, ResolvedTimeRange, TriggerMatch};
use wechat_summary_storage::TaskState;

use crate::{
    format_media_decode_limit, query_platform_history_paginated, summary_media_decode_limit,
    OperationalTask, PipelineOptions, PreparedPipelineInput, RecentObservedMessages,
    EMPTY_HISTORY_RETRY_DELAYS_MS,
};
use crate::{
    media_service, platform::PlatformWorker, runtime_log::append_runtime_log, summary_input,
};

pub(super) async fn load_summary_history(
    config: &AgentConfig,
    client: &PlatformWorker,
    incoming: &IncomingMessage,
    trigger: &TriggerMatch,
    range: &ResolvedTimeRange,
    recent_observed_messages: Option<&RecentObservedMessages>,
    media_decode_limit_override: Option<usize>,
) -> Result<Option<Vec<crate::platform::PlatformHistoryMessage>>> {
    let history_page_limit = config.history_message_limit();
    let media_decode_limit = match (
        summary_media_decode_limit(config),
        media_decode_limit_override,
    ) {
        (Some(configured), Some(remaining)) => Some(configured.min(remaining)),
        (None, Some(remaining)) => Some(remaining),
        (configured, None) => configured,
    };
    info!(
        room_id = %trigger.room_id,
        since = %range.since,
        until = %range.until,
        page_limit = history_page_limit,
        media_decode_limit = ?media_decode_limit,
        "querying platform history"
    );
    append_runtime_log(
        config,
        &format!(
            "history query started room={} since={} until={} page_limit={} media_decode_limit={}",
            trigger.room_id,
            range.since,
            range.until,
            history_page_limit,
            format_media_decode_limit(media_decode_limit)
        ),
    );
    let mut history = query_platform_history_paginated(
        config,
        client,
        &trigger.room_id,
        incoming.room_name.as_deref(),
        range.since,
        range.until,
        history_page_limit,
        media_decode_limit,
    )
    .await
    .context("querying platform chat history")?;
    if !history.is_empty() {
        return Ok(Some(history));
    }

    let observed_count = recent_observed_messages
        .map(|recent| {
            recent.count_user_text_in_range(&trigger.room_id, range.since, range.until, incoming)
        })
        .unwrap_or(0);
    if observed_count == 0 {
        return Ok(Some(history));
    }

    warn!(
        room_id = %trigger.room_id,
        observed_count,
        since = %range.since,
        until = %range.until,
        "platform history returned empty despite recent observed messages"
    );
    append_runtime_log(
        config,
        &format!(
            "history empty but recent listener saw messages room={} observed_count={} since={} until={}",
            trigger.room_id, observed_count, range.since, range.until
        ),
    );
    for (retry_index, delay_ms) in EMPTY_HISTORY_RETRY_DELAYS_MS.iter().copied().enumerate() {
        append_runtime_log(
            config,
            &format!(
                "history empty retry scheduled room={} retry={} delay_ms={}",
                trigger.room_id,
                retry_index + 1,
                delay_ms
            ),
        );
        tokio::time::sleep(StdDuration::from_millis(delay_ms)).await;
        history = query_platform_history_paginated(
            config,
            client,
            &trigger.room_id,
            incoming.room_name.as_deref(),
            range.since,
            range.until,
            history_page_limit,
            media_decode_limit,
        )
        .await
        .context("retrying platform chat history after suspicious empty result")?;
        append_runtime_log(
            config,
            &format!(
                "history empty retry completed room={} retry={} history_len={}",
                trigger.room_id,
                retry_index + 1,
                history.len()
            ),
        );
        if !history.is_empty() {
            return Ok(Some(history));
        }
    }

    client
        .send_text(
            &trigger.room_id,
            "历史读取暂时为空，但刚刚监听到该群有消息。wxdb 可能还在同步，请稍后再试。",
        )
        .await
        .context("sending suspicious empty-history message")?;
    append_runtime_log(
        config,
        &format!(
            "history suspicious empty after retries room={} observed_count={}",
            trigger.room_id, observed_count
        ),
    );
    Ok(None)
}

pub(super) async fn prepare_pipeline_input(
    config: &AgentConfig,
    client: &PlatformWorker,
    room_id: &str,
    options: &PipelineOptions,
    task: Option<&OperationalTask>,
    mut history: Vec<crate::platform::PlatformHistoryMessage>,
) -> Result<Option<PreparedPipelineInput>> {
    let media = media_service::enrich_history(config, room_id, &mut history).await?;
    let prepared = summary_input::prepare(config, history);
    let chat_messages = prepared.messages;
    if let Some(task) = task {
        summary_input::persist_sources(task, &chat_messages);
        task.set_stage(
            TaskState::Running,
            "media_processing",
            None,
            None,
            chat_messages.len() as u64,
            media.total() as u64,
        );
    }
    if prepared.total_messages == 0 {
        client
            .send_text(room_id, "这段时间没有可总结的文本聊天记录。")
            .await
            .context("sending empty-history message")?;
        return Ok(None);
    }

    info!(
        room_id,
        input_chars = prepared.llm_input.chars().count(),
        total_messages = prepared.total_messages,
        text_summary_enabled = options.text_summary_enabled,
        image_gen_enabled = options.image_gen_enabled,
        "LLM input prepared"
    );
    append_runtime_log(
        config,
        &format!(
            "llm input prepared room={} input_chars={} messages={} text_enabled={} image_enabled={}",
            room_id,
            prepared.llm_input.chars().count(),
            prepared.total_messages,
            options.text_summary_enabled,
            options.image_gen_enabled
        ),
    );
    for (label, count) in [
        ("image captions", media.images),
        ("video captions", media.videos),
        ("voice transcriptions", media.voices),
    ] {
        if count > 0 {
            append_runtime_log(
                config,
                &format!("{} inserted room={} count={}", label, room_id, count),
            );
        }
    }

    Ok(Some(PreparedPipelineInput {
        chat_messages,
        privacy: prepared.privacy,
        llm_input: prepared.llm_input,
        total_messages: prepared.total_messages,
        media,
    }))
}
