//! Image generation and platform delivery for summary artifacts.

use std::{
    collections::VecDeque,
    future::Future,
    time::{Duration as StdDuration, Instant},
};

use anyhow::{bail, Context, Result};
use tokio::sync::{mpsc as tokio_mpsc, oneshot};
use tracing::{info, warn};
use wechat_summary_ai::{OpenAiCompatibleLlm, OpenAiImageClient, RetryNotifier};
use wechat_summary_core::{models::{ChatMessage, ImageArtifact}, AgentConfig, PrivacyFilter};

use crate::{
    ai_runtime::{ai_trace_context, ai_trace_dir},
    llm_chunking::{LlmOutputLimit, LongChatCompletion},
    llm_output::looks_like_text_summary_refusal,
    llm_service::{complete_chat_summary_with_fallback, complete_llm_request_logged},
    platform::{PlatformSender, PlatformWorker},
    runtime_log::append_runtime_log,
};

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum ImagePipelineStage {
    Summary,
    Prompt,
}

impl ImagePipelineStage {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Summary => "image_summary",
            Self::Prompt => "image_prompt",
        }
    }
}

struct ImagePipelineSlotRequest {
    room_id: String,
    stage: ImagePipelineStage,
    responder: oneshot::Sender<ImagePipelineSlotLease>,
}

enum ImagePipelineSlotCommand {
    Acquire(ImagePipelineSlotRequest),
    SetCapacity(usize),
}

#[derive(Clone)]
pub(crate) struct ImagePipelineSlotPool {
    command_sender: tokio_mpsc::UnboundedSender<ImagePipelineSlotCommand>,
}

pub(crate) struct ImagePipelineSlotLease {
    release_sender: tokio_mpsc::UnboundedSender<()>,
}

impl Drop for ImagePipelineSlotLease {
    fn drop(&mut self) {
        let _ = self.release_sender.send(());
    }
}

impl ImagePipelineSlotPool {
    pub(crate) fn new(capacity: usize) -> Self {
        let (command_sender, mut command_receiver) = tokio_mpsc::unbounded_channel();
        let (release_sender, mut release_receiver) = tokio_mpsc::unbounded_channel();
        tokio::spawn(async move {
            let mut capacity = capacity.max(1);
            let mut in_use = 0usize;
            let mut prompts: VecDeque<ImagePipelineSlotRequest> = VecDeque::new();
            let mut summaries: VecDeque<ImagePipelineSlotRequest> = VecDeque::new();
            loop {
                while in_use < capacity {
                    let Some(request) = prompts.pop_front().or_else(|| summaries.pop_front())
                    else {
                        break;
                    };
                    let room_id = request.room_id.clone();
                    let stage = request.stage;
                    if request
                        .responder
                        .send(ImagePipelineSlotLease {
                            release_sender: release_sender.clone(),
                        })
                        .is_ok()
                    {
                        in_use += 1;
                        info!(
                            room_id = %room_id,
                            stage = stage.as_str(),
                            in_use,
                            capacity,
                            pending_prompts = prompts.len(),
                            pending_summaries = summaries.len(),
                            "image pipeline slot granted"
                        );
                    }
                }

                tokio::select! {
                    Some(command) = command_receiver.recv() => match command {
                        ImagePipelineSlotCommand::Acquire(request) => match request.stage {
                            ImagePipelineStage::Prompt => prompts.push_back(request),
                            ImagePipelineStage::Summary => summaries.push_back(request),
                        },
                        ImagePipelineSlotCommand::SetCapacity(value) => {
                            capacity = value.max(1);
                            info!(capacity, in_use, "image pipeline slot capacity updated");
                        }
                    },
                    Some(()) = release_receiver.recv() => {
                        in_use = in_use.saturating_sub(1);
                    },
                    else => break,
                }
            }
        });
        Self { command_sender }
    }

    pub(crate) async fn acquire(
        &self,
        room_id: &str,
        stage: ImagePipelineStage,
    ) -> Result<ImagePipelineSlotLease> {
        let (responder, receiver) = oneshot::channel();
        self.command_sender
            .send(ImagePipelineSlotCommand::Acquire(
                ImagePipelineSlotRequest {
                    room_id: room_id.to_string(),
                    stage,
                    responder,
                },
            ))
            .map_err(|_| anyhow::anyhow!("image pipeline scheduler stopped"))?;
        receiver
            .await
            .map_err(|_| anyhow::anyhow!("image pipeline scheduler stopped while waiting"))
    }

    pub(crate) fn set_capacity(&self, capacity: usize) {
        let _ = self
            .command_sender
            .send(ImagePipelineSlotCommand::SetCapacity(capacity.max(1)));
    }
}

pub(crate) async fn run_llm_stage<T, F>(
    config: &AgentConfig,
    image_pipeline_slots: &ImagePipelineSlotPool,
    room_id: &str,
    stage: ImagePipelineStage,
    future: F,
) -> Result<T>
where
    F: Future<Output = Result<T>>,
{
    let total_timeout_seconds = match stage {
        ImagePipelineStage::Summary => config.image_pipeline.summary_total_timeout_seconds,
        ImagePipelineStage::Prompt => config.image_pipeline.prompt_total_timeout_seconds,
    }
    .max(1);
    let wait_started = Instant::now();
    let lease = image_pipeline_slots.acquire(room_id, stage).await?;
    let wait_ms = wait_started.elapsed().as_millis();
    info!(
        room_id = %room_id,
        stage = stage.as_str(),
        wait_ms,
        total_timeout_seconds,
        "image pipeline LLM stage started"
    );
    append_runtime_log(
        config,
        &format!(
            "image pipeline llm stage started room={} stage={} wait_ms={} total_timeout_seconds={}",
            room_id,
            stage.as_str(),
            wait_ms,
            total_timeout_seconds
        ),
    );
    let result = tokio::time::timeout(StdDuration::from_secs(total_timeout_seconds), future).await;
    drop(lease);
    match result {
        Ok(result) => result,
        Err(_) => bail!(
            "image pipeline {} exceeded total timeout of {} seconds",
            stage.as_str(),
            total_timeout_seconds
        ),
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn complete_summary_with_refusal_retry(
    config: &AgentConfig,
    llm: &OpenAiCompatibleLlm,
    room_id: &str,
    stage: &str,
    system_prompt: &str,
    user_prompt_template: &str,
    chat_messages: &[ChatMessage],
    privacy: &PrivacyFilter,
    refusal_retry_prompt: &str,
) -> Result<LongChatCompletion> {
    let summary_result = complete_chat_summary_with_fallback(
        config,
        llm,
        room_id,
        stage,
        system_prompt,
        user_prompt_template,
        chat_messages,
        LlmOutputLimit::Unlimited,
        privacy,
    )
    .await?;
    if !looks_like_text_summary_refusal(&summary_result.output) {
        return Ok(summary_result);
    }

    log_refusal_retry(config, room_id, stage, summary_result.output.chars().count());
    let retry_system_prompt = retry_system_prompt(system_prompt, refusal_retry_prompt);
    let retry_stage = format!("{stage} safety retry");
    let retry_result = complete_chat_summary_with_fallback(
        config,
        llm,
        room_id,
        &retry_stage,
        &retry_system_prompt,
        user_prompt_template,
        chat_messages,
        LlmOutputLimit::Unlimited,
        privacy,
    )
    .await?;

    if looks_like_text_summary_refusal(&retry_result.output) {
        ensure_output_not_refusal(
            config,
            room_id,
            &format!("{stage} after safety-aware retry"),
            &retry_result.output,
        )?;
    }

    Ok(retry_result)
}

pub(crate) async fn complete_prompt_with_refusal_retry(
    config: &AgentConfig,
    llm: &OpenAiCompatibleLlm,
    room_id: &str,
    stage: &str,
    system_prompt: &str,
    user_prompt: &str,
    refusal_retry_prompt: &str,
) -> Result<String> {
    let prompt = complete_llm_request_logged(
        config,
        llm,
        room_id,
        stage,
        system_prompt,
        user_prompt.to_string(),
        LlmOutputLimit::Unlimited,
        None,
    )
    .await?;
    if !looks_like_text_summary_refusal(&prompt) {
        return Ok(prompt);
    }

    log_refusal_retry(config, room_id, stage, prompt.chars().count());
    let retry_system_prompt = retry_system_prompt(system_prompt, refusal_retry_prompt);
    let retry_stage = format!("{stage} safety_retry");
    let retry_prompt = complete_llm_request_logged(
        config,
        llm,
        room_id,
        &retry_stage,
        &retry_system_prompt,
        user_prompt.to_string(),
        LlmOutputLimit::Unlimited,
        None,
    )
    .await?;
    if looks_like_text_summary_refusal(&retry_prompt) {
        ensure_output_not_refusal(
            config,
            room_id,
            &format!("{stage} after safety-aware retry"),
            &retry_prompt,
        )?;
    }

    Ok(retry_prompt)
}

fn retry_system_prompt(system_prompt: &str, refusal_retry_prompt: &str) -> String {
    format!("{}\n\n{}", system_prompt.trim(), refusal_retry_prompt.trim())
}

fn log_refusal_retry(config: &AgentConfig, room_id: &str, stage: &str, output_chars: usize) {
    warn!(
        room_id = %room_id,
        stage,
        output_chars,
        "LLM image pipeline output looked like a refusal; retrying with safety-aware prompt"
    );
    append_runtime_log(
        config,
        &format!(
            "llm image pipeline refusal detected room={} stage={} output_chars={} retry=safety_prompt",
            room_id, stage, output_chars
        ),
    );
}

pub(crate) fn ensure_output_not_refusal(
    config: &AgentConfig,
    room_id: &str,
    stage: &str,
    output: &str,
) -> Result<()> {
    if !looks_like_text_summary_refusal(output) {
        return Ok(());
    }

    let output_chars = output.chars().count();
    warn!(
        room_id = %room_id,
        stage,
        output_chars,
        "LLM image pipeline output looked like a refusal; skipping image generation"
    );
    append_runtime_log(
        config,
        &format!(
            "llm image pipeline refusal detected room={} stage={} output_chars={} action=skip_image_generation",
            room_id, stage, output_chars
        ),
    );
    bail!("LLM returned refusal-like {stage}; skipped image generation");
}

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
