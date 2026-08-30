//! Delivery and failure-notification steps for the summary pipeline.

use anyhow::{Context, Result};
use tracing::{info, warn};
use wechat_summary_core::{config::LlmConfig, models::ChatMessage, AgentConfig};

use crate::{
    deliver_outboxed_text, format_error_chain, format_failure_message_for_chat,
    record_image_cooldown_success, summary_image, ImageCooldownRecorder, OperationalTask,
    IMAGE_PIPELINE_REFUSAL_RETRY_PROMPT,
};
use crate::{
    platform::{PlatformSender, PlatformWorker},
    summary_image::ImagePipelineSlotPool,
};

#[allow(clippy::too_many_arguments)]
pub(super) async fn run_background_image_pipeline(
    config: AgentConfig,
    sender: PlatformSender,
    room_id: String,
    llm_input: String,
    chat_messages: Vec<ChatMessage>,
    text_summary_enabled: bool,
    image_pipeline_slots: ImagePipelineSlotPool,
    image_cooldown_recorder: Option<ImageCooldownRecorder>,
    llm_config: LlmConfig,
) -> bool {
    let result: Result<()> = async {
        summary_image::run_background(
            &config,
            &sender,
            &room_id,
            &llm_input,
            &chat_messages,
            &image_pipeline_slots,
            IMAGE_PIPELINE_REFUSAL_RETRY_PROMPT,
            llm_config,
        )
        .await?;
        record_image_cooldown_success(&config, image_cooldown_recorder.as_ref(), &room_id)
    }
    .await;
    if let Err(error) = result {
        let error_message = format_error_chain(&error);
        warn!(room_id = %room_id, error = %error_message, "background image pipeline failed");
        crate::runtime_log::append_runtime_log(
            &config,
            &format!(
                "background image pipeline failed room={} error={}",
                room_id, error_message
            ),
        );
        let prefix = if text_summary_enabled {
            "文字总结已完成，但"
        } else {
            ""
        };
        if let Err(send_error) = sender
            .send_text(
                &room_id,
                &format!(
                    "{prefix}{}",
                    format_failure_message_for_chat("图片生成失败", &error_message)
                ),
            )
            .await
        {
            warn!(
                room_id = %room_id,
                error = %format_error_chain(&send_error),
                "failed to send background image failure message"
            );
        }
        return false;
    }
    true
}

pub(super) async fn send_image_failure_message(
    config: &AgentConfig,
    client: &PlatformWorker,
    room_id: &str,
    error_message: &str,
) {
    if let Err(error) = client
        .send_text(
            room_id,
            &format!(
                "文字总结已完成，但{}",
                format_failure_message_for_chat("图片生成失败", error_message)
            ),
        )
        .await
    {
        let send_error = format_error_chain(&error);
        warn!(
            room_id = %room_id,
            error = %send_error,
            "failed to send image failure message after completed text summary"
        );
        crate::runtime_log::append_runtime_log(
            config,
            &format!(
                "failed to send image failure message room={} error={}",
                room_id, send_error
            ),
        );
    }
}

pub(super) async fn send_deferred_summary_text(
    config: &AgentConfig,
    client: &PlatformWorker,
    room_id: &str,
    pending_text_reply: &mut Option<String>,
    reason: &str,
    task: Option<&OperationalTask>,
) -> Result<bool> {
    let Some(reply) = pending_text_reply.take() else {
        return Ok(false);
    };

    if let Some(task) = task {
        deliver_outboxed_text(config, task, client, room_id, &reply)
            .await
            .with_context(|| format!("sending deferred summary text {reason}"))?;
    } else {
        client
            .send_text(room_id, &reply)
            .await
            .with_context(|| format!("sending deferred summary text {reason}"))?;
    }
    info!(
        room_id = %room_id,
        reason = %reason,
        "deferred summary text sent"
    );
    crate::runtime_log::append_runtime_log(
        config,
        &format!(
            "deferred summary text sent room={} reason={}",
            room_id, reason
        ),
    );
    Ok(true)
}
