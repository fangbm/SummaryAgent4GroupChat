//! Image generation and platform delivery for summary artifacts.

use anyhow::{Context, Result};
use tracing::info;
use wechat_summary_ai::{OpenAiImageClient, RetryNotifier};
use wechat_summary_core::{models::ImageArtifact, AgentConfig};

use crate::{
    ai_runtime::{ai_trace_context, ai_trace_dir},
    platform::{PlatformSender, PlatformWorker},
    runtime_log::append_runtime_log,
};

pub(crate) async fn generate(
    config: &AgentConfig,
    room_id: &str,
    image_prompt: &str,
    retry_notifier: Option<RetryNotifier>,
) -> Result<ImageArtifact> {
    let mut image_client = OpenAiImageClient::new(config.image_gen.clone(), &config.proxy)
        .context("initializing image client")?;
    if let Some(trace_dir) = ai_trace_dir(config)? {
        image_client = image_client.with_trace_dir(trace_dir);
    }
    image_client = image_client.with_trace_context(ai_trace_context(room_id, "summary image generation"));
    if let Some(retry_notifier) = retry_notifier {
        image_client = image_client.with_retry_notifier(retry_notifier);
    }
    info!(room_id = %room_id, prompt_chars = image_prompt.chars().count(), "generating summary image");
    append_runtime_log(
        config,
        &format!(
            "generating summary image room={} prompt_chars={}",
            room_id,
            image_prompt.chars().count()
        ),
    );
    let artifact = image_client
        .generate_from_prompt(image_prompt, &config.runtime.output_dir)
        .await
        .context("generating summary image")?;
    info!(room_id = %room_id, path = %artifact.path, size_bytes = artifact.size_bytes, "summary image generated");
    append_runtime_log(
        config,
        &format!(
            "summary image generated room={} path={} size_bytes={}",
            room_id, artifact.path, artifact.size_bytes
        ),
    );
    Ok(artifact)
}

pub(crate) async fn send_with_sender(
    config: &AgentConfig,
    sender: &PlatformSender,
    room_id: &str,
    artifact: &ImageArtifact,
) -> Result<()> {
    info!(room_id = %room_id, path = %artifact.path, "sending summary image");
    sender
        .send_image(room_id, &artifact.path)
        .await
        .context("sending summary image")?;
    info!(room_id = %room_id, "summary image sent");
    append_runtime_log(config, &format!("summary image sent room={}", room_id));
    Ok(())
}

pub(crate) async fn send_with_worker(
    config: &AgentConfig,
    worker: &PlatformWorker,
    room_id: &str,
    artifact: &ImageArtifact,
) -> Result<()> {
    info!(room_id = %room_id, path = %artifact.path, "sending summary image");
    worker
        .send_image(room_id, &artifact.path)
        .await
        .context("sending summary image")?;
    info!(room_id = %room_id, "summary image sent");
    append_runtime_log(config, &format!("summary image sent room={}", room_id));
    Ok(())
}
