from linux_bot.config import TimeRangeSettings
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

