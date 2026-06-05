use std::collections::HashMap;

use chrono::{DateTime, Duration, Utc};

use crate::models::{ChatMessage, UserStat};

#[derive(Debug, Clone, PartialEq)]
pub struct FormattedChat {
    pub merged_input: String,
    pub chat_records: String,
    pub stats_text: String,
    pub total_messages: usize,
    pub duration_hours: f64,
    pub user_stats: Vec<UserStat>,
}

pub struct ChatFormatter;

impl ChatFormatter {
    pub fn format(messages: &[ChatMessage]) -> FormattedChat {
        let mut valid = messages
            .iter()
            .filter(|msg| is_text_message_type(&msg.msg_type) && !msg.content.trim().is_empty())
            .cloned()
            .collect::<Vec<_>>();
        valid.sort_by_key(|msg| msg.timestamp);

        if valid.is_empty() {
            return FormattedChat {
                merged_input: "无有效聊天记录".to_string(),
                chat_records: String::new(),
                stats_text: String::new(),
                total_messages: 0,
                duration_hours: 0.0,
                user_stats: Vec::new(),
            };
        }

        let chat_records = valid
            .iter()
            .map(|msg| {
                format!(
                    "[{}] {}: {}",
                    format_beijing_message_time(msg.timestamp),
                    msg.display_sender(),
                    msg.content.trim()
                )
            })
            .collect::<Vec<_>>()
            .join("\n");

        let total = valid.len();
        let first = valid.first().expect("valid messages").timestamp;
        let last = valid.last().expect("valid messages").timestamp;
        let duration_hours = ((last - first).num_seconds() as f64 / 3600.0).max(0.001);

        let mut counts = HashMap::<String, usize>::new();
        for msg in &valid {
            *counts.entry(msg.display_sender().to_string()).or_insert(0) += 1;
        }

        let mut user_stats = counts
            .into_iter()
            .map(|(user, count)| UserStat {
                user,
                count,
                percentage: round1(count as f64 / total as f64 * 100.0),
                frequency_per_hour: round1(count as f64 / duration_hours),
            })
            .collect::<Vec<_>>();
        user_stats.sort_by(|left, right| {
            right
                .count
                .cmp(&left.count)
                .then_with(|| left.user.cmp(&right.user))
        });

        let mut stats_lines = vec![format!(
            "===== 用户发言统计 (共 {} 条, 时长 {:.2} 小时) =====",
            total, duration_hours
        )];
        stats_lines.extend(user_stats.iter().map(|stat| {
            format!(
                "  {}: {} 条 ({:.1}%), 频率 {:.1} 条/小时",
                stat.user, stat.count, stat.percentage, stat.frequency_per_hour
            )
        }));
        let stats_text = stats_lines.join("\n");
        let merged_input = format!("[CHAT_RECORDS]\n{chat_records}\n\n[USER_STATS]\n{stats_text}");

        FormattedChat {
            merged_input,
            chat_records,
            stats_text,
            total_messages: total,
            duration_hours,
            user_stats,
        }
    }
}

fn round1(value: f64) -> f64 {
    (value * 10.0).round() / 10.0
}

fn format_beijing_message_time(value: DateTime<Utc>) -> String {
    (value + Duration::hours(8))
        .format("%m-%d %H:%M")
        .to_string()
}

fn is_text_message_type(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "text" | "1" | "文本" | "文字" | "image" | "img" | "3" | "图片" | "voice" | "语音" | "34"
    )
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};

    use super::*;

    fn chat(ts: i64, sender: &str, content: &str) -> ChatMessage {
        ChatMessage {
            timestamp: Utc.timestamp_opt(ts, 0).unwrap(),
            sender_id: sender.into(),
            sender_name: Some(sender.into()),
            content: content.into(),
            msg_type: "text".into(),
        }
    }

    #[test]
    fn formats_chat_records_and_stats() {
        let formatted = ChatFormatter::format(&[
            chat(1_716_464_760, "Bob", "第二条"),
            chat(1_716_464_700, "Alice", "第一条"),
            chat(1_716_464_820, "Alice", "第三条"),
        ]);

        assert!(formatted.merged_input.contains("[CHAT_RECORDS]"));
        assert!(formatted
            .merged_input
            .contains("[05-23 19:45] Alice: 第一条"));
        assert!(formatted.merged_input.contains("[USER_STATS]"));
        assert_eq!(formatted.total_messages, 3);
        assert_eq!(formatted.user_stats[0].user, "Alice");
        assert_eq!(formatted.user_stats[0].count, 2);
    }

    #[test]
    fn formats_chinese_text_message_type() {
        let mut message = chat(1_716_464_700, "Alice", "中文文本类型也应该保留");
        message.msg_type = "文本".into();

        let formatted = ChatFormatter::format(&[message]);

        assert_eq!(formatted.total_messages, 1);
        assert!(formatted.chat_records.contains("中文文本类型也应该保留"));
    }

    #[test]
    fn formats_voice_message_type() {
        let mut message = chat(1_716_464_700, "Alice", "[语音]（语音转写：你好）");
        message.msg_type = "voice".into();

        let formatted = ChatFormatter::format(&[message]);

        assert_eq!(formatted.total_messages, 1);
        assert!(formatted.chat_records.contains("语音转写：你好"));
    }
}
