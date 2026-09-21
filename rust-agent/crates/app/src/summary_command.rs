//! Parsing for `/总结 [platform] [time] [img] [preview]` commands.

use wechat_summary_core::{config::PlatformKindConfig, TriggerMatch};

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct SummaryCommand {
    pub(crate) target_platform: PlatformKindConfig,
    pub(crate) range_minutes: Option<i64>,
    pub(crate) image_token_present: bool,
    pub(crate) preview_only: bool,
    pub(crate) sender_filter: Option<String>,
}

pub(crate) fn parse(
    trigger: &TriggerMatch,
    default_platform: PlatformKindConfig,
) -> Option<SummaryCommand> {
    let args = trigger
        .trigger_content
        .strip_prefix(&trigger.trigger_symbol)
        .unwrap_or_default()
        .trim();
    parse_args(args, default_platform)
}

pub(crate) fn parse_args(
    args: &str,
    default_platform: PlatformKindConfig,
) -> Option<SummaryCommand> {
    let args = args.trim();
    if args.is_empty() {
        return Some(SummaryCommand {
            target_platform: default_platform,
            range_minutes: None,
            image_token_present: false,
            preview_only: false,
            sender_filter: None,
        });
    }

    let mut target_platform = default_platform;
    let mut image_token_present = false;
    let mut preview_only = false;
    let mut sender_filter = None;
    let mut range_tokens: Vec<&str> = Vec::new();
    for token in args.split_whitespace() {
        if let Some(platform) = PlatformKindConfig::parse_alias(token) {
            target_platform = platform;
        } else if is_image_token(token) {
            image_token_present = true;
        } else if is_preview_token(token) {
            preview_only = true;
        } else if let Some(sender) = token.strip_prefix('@').filter(|sender| !sender.is_empty()) {
            sender_filter = Some(sender.to_string());
        } else {
            range_tokens.push(token);
        }
    }

    let range_minutes = if range_tokens.is_empty() {
        None
    } else {
        parse_time_range_minutes(&range_tokens)?
    };

    Some(SummaryCommand {
        target_platform,
        range_minutes,
        image_token_present,
        preview_only,
        sender_filter,
    })
}

fn is_image_token(token: &str) -> bool {
    let token = token.trim();
    matches!(token, "图片") || matches!(token.to_ascii_lowercase().as_str(), "image" | "img")
}

fn is_preview_token(token: &str) -> bool {
    matches!(
        token.trim().to_ascii_lowercase().as_str(),
        "preview" | "dry-run"
    ) || matches!(token.trim(), "预览" | "试运行")
}

fn parse_time_range_minutes(tokens: &[&str]) -> Option<Option<i64>> {
    if tokens.len() == 1 && is_default_time_range_token(tokens[0]) {
        return Some(None);
    }
    parse_strict_duration_minutes(tokens).map(Some)
}

fn is_default_time_range_token(token: &str) -> bool {
    let token = token.trim();
    matches!(token.to_ascii_lowercase().as_str(), "today")
        || matches!(token, "今天" | "今日" | "本日")
}

fn parse_strict_duration_minutes(tokens: &[&str]) -> Option<i64> {
    if tokens.is_empty() || tokens.len() > 2 {
        return None;
    }

    let first = tokens[0].trim();
    if first.is_empty() {
        return None;
    }

    let split_at = first
        .char_indices()
        .find(|(_, ch)| !ch.is_ascii_digit())
        .map(|(index, _)| index)
        .unwrap_or(first.len());
    if split_at == 0 {
        return None;
    }

    let (amount, inline_unit) = first.split_at(split_at);
    let amount = amount.parse::<i64>().ok()?;
    if amount <= 0 {
        return None;
    }

    let unit = if inline_unit.is_empty() {
        if tokens.len() != 2 {
            return None;
        }
        tokens[1].trim()
    } else {
        if tokens.len() != 1 {
            return None;
        }
        inline_unit
    };

    match unit.to_ascii_lowercase().as_str() {
        "m" | "min" | "mins" | "minute" | "minutes" | "分钟" | "分钟内" | "分" | "分内" => {
            Some(amount)
        }
        "h" | "hr" | "hrs" | "hour" | "hours" | "小时" | "小时内" | "时" | "时内" => {
            Some(amount * 60)
        }
        "d" | "day" | "days" | "天" | "天内" | "日" | "日内" => Some(amount * 24 * 60),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_sender_filter_after_time_range() {
        let command = parse_args("24h @Alice", PlatformKindConfig::Wx4py).unwrap();

        assert_eq!(command.range_minutes, Some(24 * 60));
        assert_eq!(command.sender_filter.as_deref(), Some("Alice"));
    }

    #[test]
    fn parses_sender_filter_before_time_range() {
        let command = parse_args("@Alice 24h", PlatformKindConfig::Wx4py).unwrap();

        assert_eq!(command.range_minutes, Some(24 * 60));
        assert_eq!(command.sender_filter.as_deref(), Some("Alice"));
    }
}
