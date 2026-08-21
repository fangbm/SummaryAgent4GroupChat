from datetime import UTC, datetime, timedelta

from linux_bot.config import TimeRangeMode, TimeRangeSettings
from linux_bot.state_manager import GroupStateManager
from pipeline_core.storage import SQLiteStore


def test_between_triggers_persists_state(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "state.sqlite3")
    manager = GroupStateManager(store, TimeRangeSettings(fallback_minutes=30))
    first = manager.on_trigger("g", 3600)
    second = manager.on_trigger("g", 7200)
    assert first.mode == "default_fallback"
    assert second.mode == "between_triggers"
    assert int(second.since.timestamp()) == 3600


def test_today_mode_starts_at_local_midnight(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "state.sqlite3")
    manager = GroupStateManager(store, TimeRangeSettings(mode=TimeRangeMode.TODAY))
    now = datetime.now(UTC)
    current_ts = int(now.timestamp())

    result = manager.on_trigger("g", current_ts)

    local_midnight = now.astimezone().replace(hour=0, minute=0, second=0, microsecond=0)
    expected_since = local_midnight.astimezone(UTC)
    assert result.mode == "today"
    assert result.since == expected_since
    assert result.until == datetime.fromtimestamp(current_ts, tz=UTC)
    assert timedelta(0) <= result.until - result.since < timedelta(days=1)

