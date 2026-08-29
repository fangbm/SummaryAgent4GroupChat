//! LLM request orchestration for direct and long-chat summaries.

use std::time::Instant;

use anyhow::{bail, Context, Result};
use tokio::task::JoinSet;
use tracing::warn;
use wechat_summary_ai::{AiError, OpenAiCompatibleLlm};
use wechat_summary_core::{config::PrivacyConfig, models::ChatMessage, AgentConfig, PrivacyFilter};

use crate::{
    ai_runtime::{ai_trace_context, ai_trace_context_for_chunk},
    llm_chunking::{
        build_llm_chunk_requests, format_chunk_summaries_for_output, private_formatted_chat_input,
        split_llm_chunk_request, ChunkSummary, LlmChunkRequest, LlmOutputLimit,
        LongChatCompletion,
    },
    llm_output::{looks_like_text_summary_refusal, sanitize_llm_visible_output},
    render_prompt_template,
    runtime_log::{append_runtime_log, compact_ai_error_for_runtime},
};

const CONTEXT_LENGTH_SPLIT_MAX_DEPTH: usize = 12;

pub(crate) async fn complete_text_summary_with_refusal_retry(
    config: &AgentConfig,
    llm: &OpenAiCompatibleLlm,
    room_id: &str,
    chat_messages: &[ChatMessage],
    privacy: &PrivacyFilter,
    refusal_retry_prompt: &str,
) -> Result<LongChatCompletion> {
    let summary_result = complete_chat_summary_with_fallback(
        config,
        llm,
        room_id,
        "text summary",
        &config.text_summary.system_prompt,
        &config.text_summary.user_prompt_template,
        chat_messages,
        LlmOutputLimit::Configured,
        privacy,
    )
    .await?;
    if !looks_like_text_summary_refusal(&summary_result.output) {
        return Ok(summary_result);
    }

    warn!(
        room_id = %room_id,
        output_chars = summary_result.output.chars().count(),
        "LLM text summary looked like a refusal; retrying with safety-aware prompt"
    );
    append_runtime_log(
        config,
        &format!(
            "llm text summary refusal detected room={} output_chars={} retry=safety_prompt",
            room_id,
            summary_result.output.chars().count()
        ),
    );

    let retry_system_prompt = format!(
        "{}\n\n{}",
        config.text_summary.system_prompt.trim(),
        refusal_retry_prompt.trim()
    );
    let retry_result = complete_chat_summary_with_fallback(
        config,
        llm,
        room_id,
        "text summary safety retry",
        &retry_system_prompt,
        &config.text_summary.user_prompt_template,
        chat_messages,
        LlmOutputLimit::Configured,
        privacy,
    )
    .await?;

    if looks_like_text_summary_refusal(&retry_result.output) {
        bail!("LLM returned refusal-like text summary after safety-aware retry");
    }

    Ok(retry_result)
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn complete_chat_summary_with_fallback(
    config: &AgentConfig,
    llm: &OpenAiCompatibleLlm,
    room_id: &str,
    stage: &str,
    system_prompt: &str,
    user_prompt_template: &str,
    chat_messages: &[ChatMessage],
    output_limit: LlmOutputLimit,
    privacy: &PrivacyFilter,
) -> Result<LongChatCompletion> {
    let max_prompt_chars = config.privacy.max_chars_to_llm.max(1);
    let full_chat_input = private_formatted_chat_input(chat_messages, privacy);
    let full_prompt = render_prompt_template(user_prompt_template, &full_chat_input, "", "");
    let full_prompt_chars = full_prompt.chars().count();
    if full_prompt_chars <= max_prompt_chars || chat_messages.len() <= 1 {
        append_runtime_log(
            config,
            &format!(
                "calling llm {} room={} prompt_chars={} mode=direct output_limit={}",
                stage,
                room_id,
                full_prompt_chars,
                output_limit.label()
            ),
        );
        let output = complete_llm_request_logged(
            config,
            llm,
            room_id,
            stage,
            system_prompt,
            full_prompt,
            output_limit,
            None,
        )
        .await?;
        return Ok(LongChatCompletion {
            output,
            followup_chat_input: full_chat_input,
        });
    }

    let chunks = build_llm_chunk_requests(
        chat_messages,
        privacy,
        user_prompt_template,
        max_prompt_chars,
    );
    tracing::info!(
        room_id = %room_id,
        stage,
        prompt_chars = full_prompt_chars,
        max_prompt_chars,
        chunks = chunks.len(),
        "LLM long chat fallback activated"
    );
    append_runtime_log(
        config,
        &format!(
            "llm long chat fallback room={} stage={} prompt_chars={} max_chars={} chunks={}",
            room_id,
            stage,
            full_prompt_chars,
            max_prompt_chars,
            chunks.len()
        ),
    );
    let chunk_summaries = complete_chunk_requests(
        config,
        llm,
        room_id,
        stage,
        system_prompt,
        user_prompt_template,
        &config.privacy,
        &chunks,
        output_limit,
    )
    .await?;
    let combined_input = format_chunk_summaries_for_output(&chunk_summaries);
    tracing::info!(
        room_id = %room_id,
        stage,
        chunks = chunk_summaries.len(),
        output_chars = combined_input.chars().count(),
        "LLM long chat fallback completed with concatenated chunk summaries"
    );
    append_runtime_log(
        config,
        &format!(
            "llm long chat fallback concatenated room={} stage={} chunks={} output_chars={}",
            room_id,
            stage,
            chunk_summaries.len(),
            combined_input.chars().count()
        ),
    );
    let output = sanitize_llm_visible_output_with_log(config, room_id, stage, &combined_input);
    Ok(LongChatCompletion {
        output,
        followup_chat_input: combined_input,
    })
}

#[allow(clippy::too_many_arguments)]
async fn complete_chunk_requests(
    config: &AgentConfig,
    llm: &OpenAiCompatibleLlm,
    room_id: &str,
    stage: &str,
    system_prompt: &str,
    user_prompt_template: &str,
    privacy_config: &PrivacyConfig,
    chunks: &[LlmChunkRequest],
    output_limit: LlmOutputLimit,
) -> Result<Vec<ChunkSummary>> {
    let mut join_set = JoinSet::new();
    let max_concurrent = config.llm.max_concurrent_chunk_requests.max(1);
    append_runtime_log(
        config,
        &format!(
            "llm chunk batch started room={} stage={} chunks={} max_concurrent={}",
            room_id,
            stage,
            chunks.len(),
            max_concurrent
        ),
    );
    let mut next_chunk = 0usize;
    while next_chunk < chunks.len() && join_set.len() < max_concurrent {
        spawn_llm_chunk_request(
            &mut join_set,
            config,
            llm,
            room_id,
            stage,
            system_prompt,
            user_prompt_template,
            privacy_config,
            chunks,
            chunks[next_chunk].clone(),
            chunks.len(),
            output_limit,
        );
        next_chunk += 1;
    }

    let mut summaries = vec![None; chunks.len()];
    let mut first_error = None;
    while let Some(joined) = join_set.join_next().await {
        let (chunk, result) = joined.context("joining LLM chunk request task")?;
        match result {
            Ok(output) => {
                let output = sanitize_llm_visible_output_with_log(config, room_id, stage, &output);
                append_runtime_log(
                    config,
                    &format!(
                        "llm chunk completed room={} stage={} chunk={}/{} message_count={} output_chars={}",
                        room_id,
                        stage,
                        chunk.index + 1,
                        chunks.len(),
                        chunk.message_count,
                        output.chars().count()
                    ),
                );
                summaries[chunk.index] = Some(ChunkSummary {
                    index: chunk.index,
                    message_count: chunk.message_count,
                    output,
                });
            }
            Err(error) => {
                if first_error.is_none() {
                    first_error = Some((chunk.index, error));
                }
            }
        }

        while next_chunk < chunks.len() && join_set.len() < max_concurrent {
            spawn_llm_chunk_request(
                &mut join_set,
                config,
                llm,
                room_id,
                stage,
                system_prompt,
                user_prompt_template,
                privacy_config,
                chunks,
                chunks[next_chunk].clone(),
                chunks.len(),
                output_limit,
            );
            next_chunk += 1;
        }
    }

    if let Some((index, error)) = first_error {
        return Err(anyhow::Error::new(error)
            .context(format!("calling LLM for {stage} chunk {}", index + 1)));
    }

    summaries
        .into_iter()
        .enumerate()
        .map(|(index, summary)| {
            summary.with_context(|| format!("missing LLM chunk summary {}", index + 1))
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
fn spawn_llm_chunk_request(
    join_set: &mut JoinSet<(LlmChunkRequest, std::result::Result<String, AiError>)>,
    config: &AgentConfig,
    llm: &OpenAiCompatibleLlm,
    room_id: &str,
    stage: &str,
    system_prompt: &str,
    user_prompt_template: &str,
    privacy_config: &PrivacyConfig,
    chunks: &[LlmChunkRequest],
    chunk: LlmChunkRequest,
    chunk_total: usize,
    output_limit: LlmOutputLimit,
) {
    append_runtime_log(
        config,
        &format!(
            "llm chunk scheduled room={} stage={} chunk={}/{} message_count={} input_chars={} prompt_chars={}",
            room_id,
            stage,
            chunk.index + 1,
            chunks.len(),
            chunk.message_count,
            chunk.input_chars,
            chunk.prompt_chars
        ),
    );
    let llm = llm.clone();
    let runtime_config = config.clone();
    let system_prompt = system_prompt.to_string();
    let user_prompt_template = user_prompt_template.to_string();
    let privacy_config = privacy_config.clone();
    let room_id = room_id.to_string();
    let stage = stage.to_string();
    join_set.spawn(async move {
        let result = complete_llm_chunk_request_with_context_split(
            &runtime_config,
            &llm,
            &room_id,
            &stage,
            &system_prompt,
            &user_prompt_template,
            &privacy_config,
            &chunk,
            chunk_total,
            output_limit,
        )
        .await;
        (chunk, result)
    });
}

#[allow(clippy::too_many_arguments)]
async fn complete_llm_chunk_request_with_context_split(
    config: &AgentConfig,
    llm: &OpenAiCompatibleLlm,
    room_id: &str,
    stage: &str,
    system_prompt: &str,
    user_prompt_template: &str,
    privacy_config: &PrivacyConfig,
    chunk: &LlmChunkRequest,
    chunk_total: usize,
    output_limit: LlmOutputLimit,
) -> std::result::Result<String, AiError> {
    let mut pending = vec![(chunk.clone(), 0usize)];
    let mut outputs = Vec::new();
    while let Some((current, depth)) = pending.pop() {
        append_runtime_log(
            config,
            &format!(
                "llm chunk request started room={} stage={} chunk={} depth={} message_count={} input_chars={} prompt_chars={} output_limit={}",
                room_id,
                stage,
                chunk.index + 1,
                depth,
                current.message_count,
                current.input_chars,
                current.prompt_chars,
                output_limit.label()
            ),
        );
        let started = Instant::now();
        let traced_llm = llm.clone().with_trace_context(ai_trace_context_for_chunk(
            room_id,
            stage,
            chunk.index + 1,
            chunk_total,
        ));
        match complete_llm_request(&traced_llm, system_prompt, &current.prompt, output_limit).await {
            Ok(output) => {
                append_runtime_log(
                    config,
                    &format!(
                        "llm chunk request completed room={} stage={} chunk={} depth={} elapsed_ms={} output_chars={}",
                        room_id,
                        stage,
                        chunk.index + 1,
                        depth,
                        started.elapsed().as_millis(),
                        output.chars().count()
                    ),
                );
                outputs.push(output);
            }
            Err(error)
                if is_context_length_exceeded_error(&error)
                    && current.messages.len() > 1
                    && depth < CONTEXT_LENGTH_SPLIT_MAX_DEPTH =>
            {
                let Some((left, right)) =
                    split_llm_chunk_request(&current, privacy_config, user_prompt_template)
                else {
                    return Err(error);
                };
                warn!(
                    room_id = %room_id,
                    stage,
                    chunk = chunk.index + 1,
                    depth,
                    message_count = current.message_count,
                    prompt_chars = current.prompt_chars,
                    left_messages = left.message_count,
                    left_prompt_chars = left.prompt_chars,
                    right_messages = right.message_count,
                    right_prompt_chars = right.prompt_chars,
                    "LLM chunk exceeded context; splitting and retrying"
                );
                append_runtime_log(
                    config,
                    &format!(
                        "llm chunk context exceeded; split retry room={} stage={} chunk={} depth={} elapsed_ms={} messages={} prompt_chars={} left_messages={} left_prompt_chars={} right_messages={} right_prompt_chars={}",
                        room_id,
                        stage,
                        chunk.index + 1,
                        depth,
                        started.elapsed().as_millis(),
                        current.message_count,
                        current.prompt_chars,
                        left.message_count,
                        left.prompt_chars,
                        right.message_count,
                        right.prompt_chars
                    ),
                );
                pending.push((right, depth + 1));
                pending.push((left, depth + 1));
            }
            Err(error) => {
                append_runtime_log(
                    config,
                    &format!(
                        "llm chunk request failed room={} stage={} chunk={} depth={} elapsed_ms={} error={}",
                        room_id,
                        stage,
                        chunk.index + 1,
                        depth,
                        started.elapsed().as_millis(),
                        compact_ai_error_for_runtime(&error)
                    ),
                );
                return Err(error);
            }
        }
    }

    Ok(outputs
        .into_iter()
        .map(|output| output.trim().to_string())
        .filter(|output| !output.is_empty())
        .collect::<Vec<_>>()
        .join("\n\n"))
}

pub(crate) fn is_context_length_exceeded_error(error: &AiError) -> bool {
    match error {
        AiError::InvalidResponse(message) => {
            let lower = message.to_ascii_lowercase();
            lower.contains("context_length_exceeded")
                || lower.contains("maximum context length")
                || lower.contains("reduce the length of the messages")
        }
        _ => false,
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn complete_llm_request_logged(
    config: &AgentConfig,
    llm: &OpenAiCompatibleLlm,
    room_id: &str,
    stage: &str,
    system_prompt: &str,
    prompt: String,
    output_limit: LlmOutputLimit,
    trace_chunk: Option<(usize, usize)>,
) -> Result<String> {
    let system_chars = system_prompt.chars().count();
    let prompt_chars = prompt.chars().count();
    append_runtime_log(
        config,
        &format!(
            "llm request started room={room_id} stage={stage} system_chars={system_chars} prompt_chars={prompt_chars} output_limit={}",
            output_limit.label()
        ),
    );
    let started = Instant::now();
    let traced_llm = match trace_chunk {
        Some((chunk_index, chunk_total)) => llm.clone().with_trace_context(
            ai_trace_context_for_chunk(room_id, stage, chunk_index, chunk_total),
        ),
        None => llm
            .clone()
            .with_trace_context(ai_trace_context(room_id, stage)),
    };
    match complete_llm_request(&traced_llm, system_prompt, &prompt, output_limit).await {
        Ok(output) => {
            let output = sanitize_llm_visible_output_with_log(config, room_id, stage, &output);
            append_runtime_log(
                config,
                &format!(
                    "llm request completed room={room_id} stage={stage} elapsed_ms={} output_chars={}",
                    started.elapsed().as_millis(),
                    output.chars().count()
                ),
            );
            Ok(output)
        }
        Err(error) => {
            append_runtime_log(
                config,
                &format!(
                    "llm request failed room={room_id} stage={stage} elapsed_ms={} error={}",
                    started.elapsed().as_millis(),
                    compact_ai_error_for_runtime(&error)
                ),
            );
            Err(anyhow::Error::new(error))
        }
    }
}

async fn complete_llm_request(
    llm: &OpenAiCompatibleLlm,
    system_prompt: &str,
    prompt: &str,
    output_limit: LlmOutputLimit,
) -> std::result::Result<String, AiError> {
    match output_limit {
        LlmOutputLimit::Configured => llm.complete(system_prompt, prompt).await,
        LlmOutputLimit::Unlimited => llm.complete_without_max_tokens(system_prompt, prompt).await,
    }
}

fn sanitize_llm_visible_output_with_log(
    config: &AgentConfig,
    room_id: &str,
    stage: &str,
    output: &str,
) -> String {
    let sanitized = sanitize_llm_visible_output(output);
    if sanitized != output {
        warn!(
            room_id = %room_id,
            stage,
            before_chars = output.chars().count(),
            after_chars = sanitized.chars().count(),
            "LLM visible output was sanitized before use"
        );
        append_runtime_log(
            config,
            &format!(
                "llm output sanitized room={} stage={} before_chars={} after_chars={}",
                room_id,
                stage,
                output.chars().count(),
                sanitized.chars().count()
            ),
        );
    }
    sanitized
}

pub(crate) fn chat_input_for_followup_prompt(
    config: &AgentConfig,
    user_prompt_template: &str,
    full_chat_input: &str,
    fallback_chat_input: &str,
    image_summary: &str,
) -> String {
    let full_prompt_chars =
        render_prompt_template(user_prompt_template, full_chat_input, "", image_summary)
            .chars()
            .count();
    if full_prompt_chars <= config.privacy.max_chars_to_llm {
        return full_chat_input.to_string();
    }

    let fallback_prompt_chars =
        render_prompt_template(user_prompt_template, fallback_chat_input, "", image_summary)
            .chars()
            .count();
    if fallback_prompt_chars <= config.privacy.max_chars_to_llm {
        return fallback_chat_input.to_string();
    }

    format!("[IMAGE_SUMMARY]\n{}", image_summary.trim())
}
