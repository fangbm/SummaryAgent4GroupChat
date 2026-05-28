from datetime import UTC, datetime

from linux_bot.config import LinuxBotConfig, WindowsBridgeSettings
from linux_bot.service import LinuxBotService
from linux_bot.wx_bot_adapter import FakeWxBotAdapter
from pipeline_core.protocol import (
    MessageType,
    SignalMessage,
    TaskAcceptedPayload,
    TaskCompletedPayload,
)
from pipeline_core.storage import SQLiteStore


async def test_linux_service_tracks_task_and_sends_text_fallback(tmp_path) -> None:
    config = LinuxBotConfig(windows_bridge=WindowsBridgeSettings(token="token"))
    store = SQLiteStore(tmp_path / "linux.sqlite3")
    adapter = FakeWxBotAdapter()
    sent: list[SignalMessage] = []

    async def send_signal(signal: SignalMessage) -> None:
        sent.append(signal)

    service = LinuxBotService(
        config=config,
        store=store,
        adapter=adapter,
        send_signal=send_signal,
    )
    trigger = await service.handle_incoming_message(
        {
            "group_id": "g",
            "sender_id": "u",
            "sender_name": "张三",
            "content": "@总结",
            "type": "text",
            "timestamp": int(datetime(2026, 5, 23, tzinfo=UTC).timestamp()),
        }
    )
    assert trigger is not None
    request_id = trigger.payload["request_id"]
    await service.handle_worker_signal(
        SignalMessage.from_payload(
            MessageType.TASK_ACCEPTED,
            TaskAcceptedPayload(request_id=request_id),
        )
    )
    assert store.get_task(request_id)["status"] == "processing"
    await service.handle_worker_signal(
        SignalMessage.from_payload(
            MessageType.TASK_COMPLETED,
            TaskCompletedPayload(
                request_id=request_id,
                group_id="g",
                summary_text="文字摘要",
                image_file=None,
            ),
        )
    )
    assert adapter.sent_texts == [("g", "文字摘要")]
    assert store.get_task(request_id)["status"] == "sent"

