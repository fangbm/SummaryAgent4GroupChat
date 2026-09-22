//! Parsing for `/总结 [platform] [time] [img] [preview] [@sender]` commands.

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
    let tokens = args.split_whitespace().collect::<Vec<_>>();
    let mut index = 0usize;
    while index < tokens.len() {
        let token = tokens[index];
        if let Some(platform) = PlatformKindConfig::parse_alias(token) {
            target_platform = platform;
            index += 1;
        } else if is_image_token(token) {
            image_token_present = true;
            index += 1;
        } else if is_preview_token(token) {
            preview_only = true;
            index += 1;
        } else if let Some(sender) = token.strip_prefix('@').filter(|sender| !sender.is_empty()) {
            let mut sender_parts = vec![sender];
            index += 1;
            while index < tokens.len()
                && !tokens[index].starts_with('@')
                && recognized_non_sender_token_count(&tokens, index) == 0
            {
                sender_parts.push(tokens[index]);
                index += 1;
            }
            sender_filter = Some(sender_parts.join(" "));
        } else {
            let consumed = recognized_time_token_count(&tokens, index);
            if consumed == 0 {
                range_tokens.push(token);
                index += 1;
            } else {
                range_tokens.extend_from_slice(&tokens[index..index + consumed]);
                index += consumed;
            }
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

fn recognized_non_sender_token_count(tokens: &[&str], index: usize) -> usize {
    let token = tokens[index];
    if PlatformKindConfig::parse_alias(token).is_some()
        || is_image_token(token)
        || is_preview_token(token)
        || is_default_time_range_token(token)
    {
        return 1;
    }
    recognized_time_token_count(tokens, index)
}

fn recognized_time_token_count(tokens: &[&str], index: usize) -> usize {
    if parse_strict_duration_minutes(&tokens[index..index + 1]).is_some() {
        return 1;
    }
    if index + 1 < tokens.len()
        && parse_strict_duration_minutes(&tokens[index..index + 2]).is_some()
    {
        return 2;
    }
    0
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

    #[test]
    fn parses_sender_filter_with_spaces_when_trailing() {
        let command = parse_args("24h @Alice Smith", PlatformKindConfig::Wx4py).unwrap();

        assert_eq!(command.range_minutes, Some(24 * 60));
        assert_eq!(command.sender_filter.as_deref(), Some("Alice Smith"));
    }

    #[test]
    fn parses_sender_filter_with_spaces_before_time_range() {
        let command = parse_args("@Alice Smith 24h", PlatformKindConfig::Wx4py).unwrap();

        assert_eq!(command.range_minutes, Some(24 * 60));
        assert_eq!(command.sender_filter.as_deref(), Some("Alice Smith"));
    }
}
