//! Image generation and platform delivery for summary artifacts.

use std::{
    collections::VecDeque,
    future::Future,
    time::{Duration as StdDuration, Instant},
};

use anyhow::{bail, Context, Error, Result};
use tokio::sync::{mpsc as tokio_mpsc, oneshot};
use tracing::{info, warn};
use wechat_summary_ai::{OpenAiCompatibleLlm, OpenAiImageClient, RetryNotifier};
use wechat_summary_core::{
    config::LlmConfig,
    models::{ChatMessage, ImageArtifact},
    AgentConfig, PrivacyFilter,
};

use crate::{
    ai_runtime::{ai_trace_context, ai_trace_dir, configure_llm_tracing},
    llm_chunking::{LlmOutputLimit, LongChatCompletion},
    llm_output::looks_like_text_summary_refusal,
    llm_service::{
        chat_input_for_followup_prompt, complete_chat_summary_with_fallback,
        complete_llm_request_logged,
    },
    platform::{PlatformSender, PlatformWorker},
    render_prompt_template,
    runtime_log::{append_runtime_log, retry_log_notifier},
};

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum ImagePipelineStage {
    Summary,
    Prompt,
}

pub(crate) enum ForegroundPromptPreparationError {
    Summary(Error),
    Prompt(Error),
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

    log_refusal_retry(
        config,
        room_id,
        stage,
        summary_result.output.chars().count(),
    );
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
    format!(
        "{}\n\n{}",
        system_prompt.trim(),
        refusal_retry_prompt.trim()
    )
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

#[allow(clippy::too_many_arguments)]
pub(crate) async fn prepare_foreground_prompt(
    config: &AgentConfig,
    llm: &OpenAiCompatibleLlm,
    room_id: &str,
    llm_input: &str,
    chat_messages: &[ChatMessage],
    privacy: &PrivacyFilter,
    image_pipeline_slots: &ImagePipelineSlotPool,
    refusal_retry_prompt: &str,
) -> std::result::Result<String, ForegroundPromptPreparationError> {
    let image_summary_result = run_llm_stage(
        config,
        image_pipeline_slots,
        room_id,
        ImagePipelineStage::Summary,
        complete_summary_with_refusal_retry(
            config,
            llm,
            room_id,
            "image summary",
            &config.image_summary.system_prompt,
            &config.image_summary.user_prompt_template,
            chat_messages,
            privacy,
            refusal_retry_prompt,
        ),
    )
    .await
    .context("calling LLM for image summary")
    .map_err(ForegroundPromptPreparationError::Summary)?;
    let image_summary = image_summary_result.output;
    let image_prompt_chat_input = chat_input_for_followup_prompt(
        config,
        &config.image_prompt.user_prompt_template,
        llm_input,
        &image_summary_result.followup_chat_input,
        &image_summary,
    );
    info!(
        room_id = %room_id,
        output_chars = image_summary.chars().count(),
        "LLM image summary completed"
    );
    append_runtime_log(
        config,
        &format!(
            "llm image summary completed room={} output_chars={}",
            room_id,
            image_summary.chars().count()
        ),
    );
    let image_prompt_request = render_prompt_template(
        &config.image_prompt.user_prompt_template,
        &image_prompt_chat_input,
        "",
        &image_summary,
    );
    info!(
        room_id = %room_id,
        prompt_chars = image_prompt_request.chars().count(),
        "calling LLM for image prompt"
    );
    append_runtime_log(
        config,
        &format!(
            "calling llm image prompt room={} prompt_chars={}",
            room_id,
            image_prompt_request.chars().count()
        ),
    );
    let image_prompt = run_llm_stage(
        config,
        image_pipeline_slots,
        room_id,
        ImagePipelineStage::Prompt,
        complete_prompt_with_refusal_retry(
            config,
            llm,
            room_id,
            "image prompt",
            &config.image_prompt.system_prompt,
            &image_prompt_request,
            refusal_retry_prompt,
        ),
    )
    .await
    .context("calling LLM for image prompt")
    .map_err(ForegroundPromptPreparationError::Prompt)?;
    info!(
        room_id = %room_id,
        output_chars = image_prompt.chars().count(),
        "LLM image prompt completed"
    );
    append_runtime_log(
        config,
        &format!(
            "llm image prompt completed room={} output_chars={}",
            room_id,
            image_prompt.chars().count()
        ),
    );
    Ok(image_prompt)
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_background(
    config: &AgentConfig,
    sender: &PlatformSender,
    room_id: &str,
    llm_input: &str,
    chat_messages: &[ChatMessage],
    image_pipeline_slots: &ImagePipelineSlotPool,
    refusal_retry_prompt: &str,
    llm_config: LlmConfig,
) -> Result<()> {
    let retry_notifier = retry_log_notifier(config, room_id.to_string());
    let llm = configure_llm_tracing(
        OpenAiCompatibleLlm::new(llm_config, &config.proxy)
            .context("initializing LLM client for background image pipeline")?,
        config,
    )
    .context("configuring LLM trace output for background image pipeline")?
    .with_retry_notifier(retry_notifier.clone())
    // This background path is used after manual text summaries. Keep its
    // image-preparation completions on normal JSON responses as well.
    .with_streaming(false);
    let privacy = PrivacyFilter::new(config.privacy.clone());
    let image_summary_result = run_llm_stage(
        config,
        image_pipeline_slots,
        room_id,
        ImagePipelineStage::Summary,
        complete_summary_with_refusal_retry(
            config,
            &llm,
            room_id,
            "background image summary",
            &config.image_summary.system_prompt,
            &config.image_summary.user_prompt_template,
            chat_messages,
            &privacy,
            refusal_retry_prompt,
        ),
    )
    .await
    .context("calling LLM for background image summary")?;
    let image_summary = image_summary_result.output;
    info!(
        room_id = %room_id,
        output_chars = image_summary.chars().count(),
        "LLM background image summary completed"
    );
    append_runtime_log(
        config,
        &format!(
            "llm background image summary completed room={} output_chars={}",
            room_id,
            image_summary.chars().count()
        ),
    );

    let image_prompt_chat_input = chat_input_for_followup_prompt(
        config,
        &config.image_prompt.user_prompt_template,
        llm_input,
        &image_summary_result.followup_chat_input,
        &image_summary,
    );
    let image_prompt_request = render_prompt_template(
        &config.image_prompt.user_prompt_template,
        &image_prompt_chat_input,
        "",
        &image_summary,
    );
    info!(
        room_id = %room_id,
        prompt_chars = image_prompt_request.chars().count(),
        "calling LLM for background image prompt"
    );
    append_runtime_log(
        config,
        &format!(
            "calling llm background image prompt room={} prompt_chars={}",
            room_id,
            image_prompt_request.chars().count()
        ),
    );
    let image_prompt = run_llm_stage(
        config,
        image_pipeline_slots,
        room_id,
        ImagePipelineStage::Prompt,
        complete_prompt_with_refusal_retry(
            config,
            &llm,
            room_id,
            "background image prompt",
            &config.image_prompt.system_prompt,
            &image_prompt_request,
            refusal_retry_prompt,
        ),
    )
    .await
    .context("calling LLM for background image prompt")?;
    info!(
        room_id = %room_id,
        output_chars = image_prompt.chars().count(),
        "LLM background image prompt completed"
    );
    append_runtime_log(
        config,
        &format!(
            "llm background image prompt completed room={} output_chars={}",
            room_id,
            image_prompt.chars().count()
        ),
    );

    let artifact = generate(config, room_id, &image_prompt, Some(retry_notifier)).await?;
    send_with_sender(config, sender, room_id, &artifact).await?;
    append_runtime_log(
        config,
        &format!("background image pipeline completed room={}", room_id),
    );
    Ok(())
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
    image_client =
        image_client.with_trace_context(ai_trace_context(room_id, "summary image generation"));
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

/// Turn a short user request into a NovelAI V5-oriented prompt, then generate the image.
/// This intentionally uses the normal image-pipeline slot pool so manual commands cannot stampede
/// the same provider used by scheduled summary images.
pub(crate) async fn generate_manual_novelai_image(
    config: &AgentConfig,
    image_pipeline_slots: &ImagePipelineSlotPool,
    room_id: &str,
    user_prompt: &str,
) -> Result<ImageArtifact> {
    let provider = config.image_gen.provider.trim().to_ascii_lowercase();
    if !matches!(provider.as_str(), "novelai" | "nai") {
        bail!("图片命令需要将 [image_gen].provider 设置为 novelai 或 nai");
    }
    if !config.image_gen.enabled {
        bail!("图片生成功能未启用");
    }
    let retry_notifier = retry_log_notifier(config, room_id.to_string());
    let llm = configure_llm_tracing(
        OpenAiCompatibleLlm::new(config.llm.clone(), &config.proxy)
            .context("initializing LLM client for manual NovelAI image command")?,
        config,
    )?
    .with_retry_notifier(retry_notifier.clone())
    .with_streaming(false);
    let request = format!(
        "用户想画的内容：\n{}\n\n请直接给出最终 NovelAI 正向提示词。",
        user_prompt.trim()
    );
    let prompt = run_llm_stage(
        config,
        image_pipeline_slots,
        room_id,
        ImagePipelineStage::Prompt,
        complete_prompt_with_refusal_retry(
            config,
            &llm,
            room_id,
            "manual NovelAI image prompt",
            MANUAL_NOVELAI_PROMPT_SYSTEM,
            &request,
            MANUAL_NOVELAI_REFUSAL_RETRY,
        ),
    )
    .await
    .context("calling LLM for manual NovelAI image prompt")?;
    generate(config, room_id, &prompt, Some(retry_notifier)).await
}

const MANUAL_NOVELAI_PROMPT_SYSTEM: &str = r#"
你是 NovelAI Diffusion V5 的提示词设计师。把用户的一句话扩展为一幅单张、可直接出图的动漫插画。

只输出英文正向 prompt，不要解释、不要 Markdown、不要 JSON、不要 negative prompt、不要参数。用英文标签和简洁短语，以逗号分隔。
从画面主体开始，依次给出：质量与风格、人物数量与主体、外貌/服装、清晰动作或瞬间、镜头构图、场景、光线和色彩。补足用户没有说明但画面必需的安全细节，保持一个明确的视觉中心。
单人不要虚构 Character 字段。多人时用 Character 1、Character 2 等编号区分人物，并用 source# / target# / mutual# 关系短语明确谁在做什么，避免把两人的外貌或动作混在一起。动作、镜头和关系需要写成自然短句；发色、服装、物件和环境优先使用标签式短语。不要生成文字、水印、UI、签名或不需要的敏感细节。
"#;

const MANUAL_NOVELAI_REFUSAL_RETRY: &str = r#"
即使原始想法涉及争议或不适合直接复现的元素，也请改写为中性、安全的动漫场景提示词。不要拒绝，不要解释，只输出英文正向 prompt。
"#;

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
