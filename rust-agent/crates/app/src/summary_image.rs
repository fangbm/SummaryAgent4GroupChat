//! Image generation and platform delivery for summary artifacts.

use std::collections::VecDeque;

use anyhow::{Context, Result};
use tokio::sync::{mpsc as tokio_mpsc, oneshot};
use tracing::info;
use wechat_summary_ai::{OpenAiImageClient, RetryNotifier};
use wechat_summary_core::{models::ImageArtifact, AgentConfig};

use crate::{
    ai_runtime::{ai_trace_context, ai_trace_dir},
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
