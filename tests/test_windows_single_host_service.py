from datetime import UTC, datetime

from pipeline_core.protocol import MessageType, TaskStatus
from pipeline_core.storage import SQLiteStore
from windows_worker.config import SecuritySettings, WorkerConfig
from windows_worker.providers.image_gen import PlaceholderImageGenClient
from windows_worker.providers.llm import MockLLMClient
from windows_worker.single_host_service import WindowsSingleHostService, WindowsTimeRangeManager
from windows_worker.task_processor import TaskProcessor
from windows_worker.trigger import WindowsTriggerMatcher
from windows_worker.wechat_adapter import FakeWindowsWechatAdapter, WechatMessage
from windows_worker.wx_cli_adapter import FakeWxClient


def test_windows_trigger_matcher_filters_and_matches_prefix() -> None:
    config = WorkerConfig(
        security=SecuritySettings(ipc_token="x8Kp2vQm5tRw9zLc", download_secret="y3Nf7bHd1sJg6vKa")
    )
    config.wechat.listen.whitelist_groups = ["AI交流群"]
    config.wechat.listen.blacklist_users = ["wxid_bot"]
    matcher = WindowsTriggerMatcher(config.wechat.listen)
    message = WechatMessage(
        group_id="123@chatroom",
        group_name="AI交流群一",
        sender_id="wxid_user",
        sender_name="张三",
        content="/总结 今天",
        type="text",
        timestamp=1_769_000_000,
    )

    match = matcher.match(message)

    assert match is not None
    assert match.group_id == "123@chatroom"
    assert match.trigger_symbol == "/总结"
    assert matcher.match(message.__class__(**{**message.__dict__, "sender_id": "wxid_bot"})) is None


def test_windows_time_range_uses_last_trigger(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "worker.sqlite3")
    config = WorkerConfig(
        security=SecuritySettings(ipc_token="x8Kp2vQm5tRw9zLc", download_secret="y3Nf7bHd1sJg6vKa")
    )
    manager = WindowsTimeRangeManager(store, config.wechat.time_range)

    since, until, mode, last_trigger = manager.on_trigger("g", 1_769_000_000)
    second_since, _, second_mode, second_last_trigger = manager.on_trigger("g", 1_769_000_600)

    assert mode == "default_fallback"
    assert until > since
    assert last_trigger is None
    assert second_mode == "between_triggers"
    assert second_since == datetime.fromtimestamp(1_769_000_000, tz=UTC)
    assert second_last_trigger == second_since


def test_windows_time_range_today_starts_at_local_midnight(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "worker.sqlite3")
    config = WorkerConfig(
        security=SecuritySettings(ipc_token="x8Kp2vQm5tRw9zLc", download_secret="y3Nf7bHd1sJg6vKa")
    )
    config.wechat.time_range.mode = "today"
    manager = WindowsTimeRangeManager(store, config.wechat.time_range)
    now = datetime.now(UTC)
    current_ts = int(now.timestamp())

    since, until, mode, _ = manager.on_trigger("g", current_ts)

    local_midnight = now.astimezone().replace(hour=0, minute=0, second=0, microsecond=0)
    assert mode == "today"
    assert since == local_midnight.astimezone(UTC)
    assert until == datetime.fromtimestamp(current_ts, tz=UTC)


async def test_windows_single_host_service_processes_and_replies_text(tmp_path) -> None:
    config = WorkerConfig(
        security=SecuritySettings(ipc_token="x8Kp2vQm5tRw9zLc", download_secret="y3Nf7bHd1sJg6vKa")
    )
    config.image_gen.enabled = False
    config.wechat.listen.whitelist_groups = ["g"]
    store = SQLiteStore(tmp_path / "worker.sqlite3")
    wechat = FakeWindowsWechatAdapter()
    processor = TaskProcessor(
        config=config,
        store=store,
        wx_client=FakeWxClient(
            [
                {
                    "type": "text",
                    "timestamp": 1_769_000_000,
                    "sender_id": "wxid_a",
                    "sender_name": "张三",
                    "content": "我们今天讨论了 Windows 单机架构",
                }
            ]
        ),
        llm_client=MockLLMClient(),
        image_client=PlaceholderImageGenClient(),
    )
    service = WindowsSingleHostService(
        config=config,
        store=store,
        wechat=wechat,
        processor=processor,
    )
    message = WechatMessage(
        group_id="g",
        group_name="AI交流群",
        sender_id="wxid_user",
        sender_name="李四",
        content="/总结",
        type="text",
        timestamp=1_769_000_600,
    )

    result = await service.handle_message(message)

    assert result is not None
    assert result.type == MessageType.TASK_COMPLETED
    assert wechat.sent_texts
    assert wechat.sent_texts[0][0] == "g"
    task = store.get_task(result.payload["request_id"])
    assert task is not None
    assert task["status"] == TaskStatus.SENT.value
