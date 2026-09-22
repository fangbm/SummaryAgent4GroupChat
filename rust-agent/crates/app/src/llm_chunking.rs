//! Pure data preparation for long-chat LLM requests.

use wechat_summary_core::{
    config::PrivacyConfig, models::ChatMessage, ChatFormatter, PrivacyFilter,
};

use crate::{format_local_time, render_prompt_template};

const CHUNK_PROMPT_HEADROOM_CHARS: usize = 4_096;

#[derive(Debug, Clone)]
pub(crate) struct LongChatCompletion {
    pub(crate) output: String,
    pub(crate) followup_chat_input: String,
}

#[derive(Debug, Clone)]
pub(crate) struct LlmChunkRequest {
    pub(crate) index: usize,
    pub(crate) message_count: usize,
    pub(crate) input_chars: usize,
    pub(crate) prompt_chars: usize,
    pub(crate) messages: Vec<ChatMessage>,
    pub(crate) prompt: String,
}

#[derive(Debug, Clone)]
pub(crate) struct ChunkSummary {
    pub(crate) index: usize,
    pub(crate) message_count: usize,
    pub(crate) output: String,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum LlmOutputLimit {
    Configured,
    Unlimited,
}

impl LlmOutputLimit {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Configured => "configured",
            Self::Unlimited => "unlimited",
        }
    }
}

pub(crate) fn split_llm_chunk_request(
    chunk: &LlmChunkRequest,
    privacy_config: &PrivacyConfig,
    user_prompt_template: &str,
) -> Option<(LlmChunkRequest, LlmChunkRequest)> {
    if chunk.messages.len() <= 1 {
        return None;
    }
    let midpoint = chunk.messages.len() / 2;
    if midpoint == 0 || midpoint >= chunk.messages.len() {
        return None;
    }

    let privacy = PrivacyFilter::new(privacy_config.clone());
    let left = build_llm_chunk_request(
        chunk.index,
        chunk.messages[..midpoint].to_vec(),
        &privacy,
        user_prompt_template,
    );
    let right = build_llm_chunk_request(
        chunk.index,
        chunk.messages[midpoint..].to_vec(),
        &privacy,
        user_prompt_template,
    );
    Some((left, right))
}

pub(crate) fn build_llm_chunk_requests(
    messages: &[ChatMessage],
    privacy: &PrivacyFilter,
    user_prompt_template: &str,
    max_prompt_chars: usize,
) -> Vec<LlmChunkRequest> {
    let mut sorted = messages.to_vec();
    sorted.sort_by_key(|message| message.timestamp);
    let prompt_overhead = render_prompt_template(user_prompt_template, "", "", "")
        .chars()
        .count();
    let line_budget = max_prompt_chars
        .saturating_sub(prompt_overhead)
        .saturating_sub(CHUNK_PROMPT_HEADROOM_CHARS)
        .max(1);

    let mut rough_chunks = Vec::<Vec<ChatMessage>>::new();
    let mut current = Vec::<ChatMessage>::new();
    let mut current_chars = 0usize;
    for message in sorted {
        let line_chars = formatted_chat_line_chars(&message);
        if !current.is_empty()
            && current_chars.saturating_add(line_chars).saturating_add(1) > line_budget
        {
            rough_chunks.push(std::mem::take(&mut current));
            current_chars = 0;
        }
        current_chars = current_chars.saturating_add(line_chars).saturating_add(1);
        current.push(message);
    }
    if !current.is_empty() {
        rough_chunks.push(current);
    }

    let mut fitted_chunks = Vec::<Vec<ChatMessage>>::new();
    for chunk in rough_chunks {
        push_fitted_llm_chunks(
            chunk,
            privacy,
            user_prompt_template,
            max_prompt_chars,
            &mut fitted_chunks,
        );
    }
    fitted_chunks
        .into_iter()
        .enumerate()
        .map(|(index, messages)| {
            build_llm_chunk_request(index, messages, privacy, user_prompt_template)
        })
        .collect()
}

pub(crate) fn build_llm_chunk_request(
    index: usize,
    messages: Vec<ChatMessage>,
    privacy: &PrivacyFilter,
    user_prompt_template: &str,
) -> LlmChunkRequest {
    let input = private_formatted_chat_input(&messages, privacy);
    let prompt = render_prompt_template(user_prompt_template, &input, "", "");
    LlmChunkRequest {
        index,
        message_count: messages.len(),
        input_chars: input.chars().count(),
        prompt_chars: prompt.chars().count(),
        messages,
        prompt,
    }
}

fn push_fitted_llm_chunks(
    chunk: Vec<ChatMessage>,
    privacy: &PrivacyFilter,
    user_prompt_template: &str,
    max_prompt_chars: usize,
    output: &mut Vec<Vec<ChatMessage>>,
) {
    if chunk.len() <= 1 {
        output.push(chunk);
        return;
    }
    let input = private_formatted_chat_input(&chunk, privacy);
    let prompt_chars = render_prompt_template(user_prompt_template, &input, "", "")
        .chars()
        .count();
    if prompt_chars <= max_prompt_chars {
        output.push(chunk);
        return;
    }
    let midpoint = chunk.len() / 2;
    let right = chunk[midpoint..].to_vec();
    let left = chunk[..midpoint].to_vec();
    push_fitted_llm_chunks(
        left,
        privacy,
        user_prompt_template,
        max_prompt_chars,
        output,
    );
    push_fitted_llm_chunks(
        right,
        privacy,
        user_prompt_template,
        max_prompt_chars,
        output,
    );
}

pub(crate) fn private_formatted_chat_input(
    messages: &[ChatMessage],
    privacy: &PrivacyFilter,
) -> String {
    privacy.apply(&ChatFormatter::format(messages).merged_input)
}

fn formatted_chat_line_chars(message: &ChatMessage) -> usize {
    format!(
        "[{}] {}: {}",
        format_local_time(message.timestamp),
        message.display_sender(),
        message.content.trim()
    )
    .chars()
    .count()
}

pub(crate) fn format_chunk_summaries_for_output(summaries: &[ChunkSummary]) -> String {
    let mut parts = vec![
        "[CHUNK_SUMMARIES]".to_string(),
        "以下是同一段群聊按时间顺序切分后的分段总结。".to_string(),
    ];
    for summary in summaries {
        parts.push(format!(
            "===== 分段 {}/{}，{} 条 =====\n{}",
            summary.index + 1,
            summaries.len(),
            summary.message_count,
            summary.output.trim()
        ));
    }
    parts.join("\n\n")
}
