//! Rules for identifying the command that triggered a summary and agent-authored status rows.

use wechat_summary_core::models::IncomingMessage;
use crate::platform::PlatformHistoryMessage;

pub(crate) fn is_current_trigger(message: &PlatformHistoryMessage, incoming: &IncomingMessage) -> bool {
    message.timestamp == incoming.timestamp
        && (message.stable_id.as_deref().zip(incoming.stable_id.as_deref())
            .is_some_and(|(left, right)| stable_ids_match(left, right))
            || (message.stable_id.is_none() && incoming.stable_id.is_none()
                && message.content.trim() == incoming.content.trim()
                && (message.sender_id == incoming.sender_id || message.is_self)))
}

pub(crate) fn is_current_incoming(message: &IncomingMessage, incoming: &IncomingMessage) -> bool {
    message.timestamp == incoming.timestamp
        && (message.stable_id.as_deref().zip(incoming.stable_id.as_deref())
            .is_some_and(|(left, right)| stable_ids_match(left, right))
            || (message.stable_id.is_none() && incoming.stable_id.is_none()
                && message.content.trim() == incoming.content.trim() && message.sender_id == incoming.sender_id))
}

pub(crate) fn is_agent_status(message: &PlatformHistoryMessage) -> bool { is_agent_status_content(&message.content) }

pub(crate) fn is_agent_status_content(content: &str) -> bool {
    let content = content.trim();
    content.starts_with("收到 /总结") || content.starts_with("收到 #总结")
        || content.starts_with("总结失败：") || content.starts_with("这段时间没有可总结的文本聊天记录")
        || content.starts_with("历史读取暂时为空") || content.starts_with("当前配置未开启文字总结或图片生成")
        || content.starts_with("群聊总结（") || content.contains("暂时失败，正在重试")
}

fn stable_ids_match(left: &str, right: &str) -> bool {
    left == right || stable_id_number(left).is_some() && stable_id_number(left) == stable_id_number(right)
}
fn stable_id_number(value: &str) -> Option<u64> { value.rsplit_once(':').map(|(_, id)| id).unwrap_or(value).parse().ok() }
