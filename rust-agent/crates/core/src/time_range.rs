use std::str::FromStr;

use chrono::{DateTime, Datelike, Duration, TimeZone, Utc};

use crate::config::{TimeRangeConfig, TimeRangeMode};

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ResolvedTimeRange {
    pub since: DateTime<Utc>,
    pub until: DateTime<Utc>,
    pub mode: TimeRangeMode,
}

pub struct TimeRangeCalculator;

impl TimeRangeCalculator {
    pub fn resolve(
        now: DateTime<Utc>,
        last_trigger: Option<DateTime<Utc>>,
        config: &TimeRangeConfig,
    ) -> ResolvedTimeRange {
        let since = match config.mode {
            TimeRangeMode::BetweenTriggers => {
                last_trigger.unwrap_or_else(|| now - Duration::minutes(config.fallback_minutes))
            }
            TimeRangeMode::FixedMinutes => now - Duration::minutes(config.fixed_minutes),
            TimeRangeMode::FixedHours => now - Duration::hours(config.fixed_hours),
            TimeRangeMode::Today => Utc
                .with_ymd_and_hms(now.year(), now.month(), now.day(), 0, 0, 0)
                .single()
                .expect("valid start of day"),
        };

        ResolvedTimeRange {
            since,
            until: now,
            mode: config.mode,
        }
    }

    pub fn resolve_with_override(
        now: DateTime<Utc>,
        last_trigger: Option<DateTime<Utc>>,
        config: &TimeRangeConfig,
        override_minutes: Option<i64>,
    ) -> ResolvedTimeRange {
        if let Some(minutes) = override_minutes {
            return ResolvedTimeRange {
                since: now - Duration::minutes(minutes),
                until: now,
                mode: config.mode,
            };
        }

        Self::resolve(now, last_trigger, config)
    }
}

pub fn parse_command_time_range_minutes(input: &str) -> Option<i64> {
    let mut tokens = input.split_whitespace();
    let token = tokens.next()?.trim();
    if token.is_empty() {
        return None;
    }

    let lower = token.to_ascii_lowercase();
    if matches!(lower.as_str(), "today") || matches!(token, "今天" | "今日" | "本日") {
        return None;
    }

    let split_at = token
        .char_indices()
        .find(|(_, ch)| !ch.is_ascii_digit())
        .map(|(index, _)| index)
        .unwrap_or(token.len());
    if split_at == 0 || (split_at == token.len() && tokens.clone().next().is_none()) {
        return None;
    }

    let (amount, unit) = token.split_at(split_at);
    let amount = i64::from_str(amount).ok()?;
    if amount <= 0 {
        return None;
    }

    let unit = if unit.is_empty() {
        tokens.next().unwrap_or_default()
    } else {
        unit
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
    use chrono::{TimeZone, Utc};

    use super::*;

    #[test]
    fn between_triggers_falls_back_on_first_trigger() {
        let now = Utc.timestamp_opt(1_716_464_700, 0).unwrap();
        let cfg = TimeRangeConfig {
            mode: TimeRangeMode::BetweenTriggers,
            fallback_minutes: 30,
            fixed_minutes: 10,
            fixed_hours: 2,
        };

        let range = TimeRangeCalculator::resolve(now, None, &cfg);
        assert_eq!(range.since, now - Duration::minutes(30));
    }

    #[test]
    fn command_time_range_supports_minutes_and_hours() {
        assert_eq!(parse_command_time_range_minutes("30min"), Some(30));
        assert_eq!(parse_command_time_range_minutes("30 min"), Some(30));
        assert_eq!(parse_command_time_range_minutes("30分钟"), Some(30));
        assert_eq!(parse_command_time_range_minutes("30 分钟"), Some(30));
        assert_eq!(parse_command_time_range_minutes("30分钟内"), Some(30));
        assert_eq!(parse_command_time_range_minutes("1h"), Some(60));
        assert_eq!(parse_command_time_range_minutes("1 h"), Some(60));
        assert_eq!(parse_command_time_range_minutes("1小时"), Some(60));
        assert_eq!(parse_command_time_range_minutes("1 小时"), Some(60));
        assert_eq!(parse_command_time_range_minutes("1小时内"), Some(60));
        assert_eq!(parse_command_time_range_minutes("2小时"), Some(120));
        assert_eq!(parse_command_time_range_minutes("1天"), Some(24 * 60));
        assert_eq!(parse_command_time_range_minutes("1 天"), Some(24 * 60));
        assert_eq!(parse_command_time_range_minutes("1天内"), Some(24 * 60));
        assert_eq!(parse_command_time_range_minutes("刚才说了什么"), None);
    }

    #[test]
    fn command_time_range_overrides_config() {
        let now = Utc.timestamp_opt(1_716_464_700, 0).unwrap();
        let cfg = TimeRangeConfig {
            mode: TimeRangeMode::BetweenTriggers,
            fallback_minutes: 30,
            fixed_minutes: 10,
            fixed_hours: 2,
        };

        let range = TimeRangeCalculator::resolve_with_override(now, None, &cfg, Some(90));

        assert_eq!(range.since, now - Duration::minutes(90));
    }
}
