//! Turns platform history into privacy-filtered LLM input and stable source references.

use wechat_summary_core::{models::ChatMessage, AgentConfig, ChatFormatter, PrivacyFilter};
use wechat_summary_storage::SourceReference;

use crate::{history_to_chat_message, platform::PlatformHistoryMessage, OperationalTask};

pub(crate) struct PreparedSummaryInput {
    pub(crate) messages: Vec<ChatMessage>,
    pub(crate) total_messages: usize,
    pub(crate) llm_input: String,
    pub(crate) privacy: PrivacyFilter,
}

pub(crate) fn prepare(
    config: &AgentConfig,
    history: Vec<PlatformHistoryMessage>,
) -> PreparedSummaryInput {
    let messages = history
        .into_iter()
        .map(history_to_chat_message)
        .collect::<Vec<_>>();
    let formatted = ChatFormatter::format(&messages);
    let privacy = PrivacyFilter::new(config.privacy.clone());
    let llm_input = privacy.apply(&formatted.merged_input);
    PreparedSummaryInput {
        messages,
        total_messages: formatted.total_messages,
        llm_input,
        privacy,
    }
}

pub(crate) fn persist_sources(task: &OperationalTask, messages: &[ChatMessage]) {
    let references = messages
        .iter()
        .enumerate()
        .map(|(index, message)| SourceReference {
            task_id: task.id.clone(),
            point_index: 0,
            source_id: format!("m-{}", index + 1),
            occurred_at: message.timestamp,
            sender_label: format!("成员-{:x}", md5::compute(message.sender_id.as_bytes())),
            message_index: (index + 1) as u32,
        })
        .collect::<Vec<_>>();
    if let Err(error) = task.store.add_source_references(&references) {
        tracing::warn!(task_id = %task.id, error = %error, "failed to store task source references");
    }
}
