from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from linux_bot.config import TimeRangeMode, TimeRangeSettings
from pipeline_core.storage import SQLiteStore


@dataclass(frozen=True)
class TimeRange:
    since: datetime
    until: datetime
    mode: str
    last_trigger_time: datetime | None


def _dt_from_ts(timestamp: int) -> datetime:
    return datetime.fromtimestamp(timestamp, tz=UTC)


class GroupStateManager:
    def __init__(self, store: SQLiteStore, settings: TimeRangeSettings):
        self.store = store
        self.settings = settings

    def on_trigger(self, group_id: str, current_ts: int) -> TimeRange:
        until = _dt_from_ts(current_ts)
        last_ts = self.store.get_last_trigger(group_id)
        last_dt = _dt_from_ts(last_ts) if last_ts else None

        if self.settings.mode == TimeRangeMode.FIXED_MINUTES:
            since = until - timedelta(minutes=self.settings.fixed_minutes)
            mode = TimeRangeMode.FIXED_MINUTES.value
        elif self.settings.mode == TimeRangeMode.FIXED_HOURS:
            since = until - timedelta(hours=self.settings.fixed_hours)
            mode = TimeRangeMode.FIXED_HOURS.value
        elif self.settings.mode == TimeRangeMode.TODAY:
            since = until.replace(hour=0, minute=0, second=0, microsecond=0)
            mode = TimeRangeMode.TODAY.value
        elif last_ts:
            since = last_dt or until
            mode = TimeRangeMode.BETWEEN_TRIGGERS.value
        else:
            since = until - timedelta(minutes=self.settings.fallback_minutes)
            mode = "default_fallback"

        self.store.set_last_trigger(group_id, current_ts)
        return TimeRange(since=since, until=until, mode=mode, last_trigger_time=last_dt)

