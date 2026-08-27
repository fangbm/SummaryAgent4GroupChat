//! Pure weekly-report scheduling rules, isolated from report generation and I/O.

use std::collections::HashMap;

use chrono::{DateTime, Datelike, Duration, Local, TimeZone, Utc};
use wechat_summary_core::{config::ReportGroupConfig, AgentConfig};
use wechat_summary_storage::{TaskRecord, TaskState};

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct WeeklyStats {
    pub(crate) tasks: usize,
    pub(crate) succeeded: usize,
    pub(crate) failed: usize,
    pub(crate) messages: u64,
    pub(crate) media: u64,
}

pub(crate) fn aggregate(tasks: impl IntoIterator<Item = impl std::borrow::Borrow<TaskRecord>>) -> WeeklyStats {
    tasks.into_iter().fold(WeeklyStats::default(), |mut stats, task| {
        let task = task.borrow();
        stats.tasks += 1;
        stats.succeeded += usize::from(task.state == TaskState::Succeeded);
        stats.failed += usize::from(task.state == TaskState::Failed);
        stats.messages += task.message_count;
        stats.media += task.media_count;
        stats
    })
}

pub(crate) fn schedule(now: DateTime<Utc>, config: &AgentConfig) -> HashMap<String, DateTime<Utc>> {
    config.report_groups.iter()
        .filter(|(_, group)| group.enabled && !group.rooms.is_empty())
        .filter_map(|(name, group)| next_run_after(now, group).map(|run_at| (name.clone(), run_at)))
        .collect()
}

pub(crate) fn next_run_after(now: DateTime<Utc>, group: &ReportGroupConfig) -> Option<DateTime<Utc>> {
    if group.weekday > 6 || group.local_hour > 23 || group.local_minute > 59 { return None; }
    let local_now = now.with_timezone(&Local);
    let days = (group.weekday + 7 - local_now.weekday().num_days_from_monday()) % 7;
    let candidate = Local.from_local_datetime(
        &(local_now.date_naive() + Duration::days(days as i64))
            .and_hms_opt(group.local_hour, group.local_minute, 0)?
    ).single()?;
    Some(if candidate <= local_now { (candidate + Duration::days(7)).with_timezone(&Utc) } else { candidate.with_timezone(&Utc) })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rolls_weekly_run_forward_after_today_time_has_passed() {
        let group = ReportGroupConfig { enabled: true, rooms: vec!["room".into()], weekday: 0, local_hour: 9, local_minute: 0 };
        let now = "2026-08-24T02:00:00Z".parse().unwrap();
        assert!(next_run_after(now, &group).unwrap() > now);
    }
}
