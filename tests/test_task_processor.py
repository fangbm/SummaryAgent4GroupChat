from datetime import UTC, datetime

from pipeline_core.errors import ErrorCode, PipelineError
from pipeline_core.protocol import MessageType, SignalMessage, TriggerDetectedPayload
from pipeline_core.storage import SQLiteStore
from windows_worker.config import SecuritySettings, WorkerConfig
from windows_worker.providers.image_gen import PlaceholderImageGenClient
from windows_worker.providers.llm import MockLLMClient
from windows_worker.task_processor import TaskProcessor
from windows_worker.wx_cli_adapter import FakeWxClient


class CloudLLMClient:
    is_local = False

    async def summarize(self, merged_input: str) -> str:
        return "cloud summary"


class BrokenImageClient:
    async def generate(self, summary: str, output_path):
        raise PipelineError(ErrorCode.IMAGE_GEN_FAILED, "boom", retryable=False)


async def test_task_processor_success_with_placeholder_image(tmp_path) -> None:
    config = WorkerConfig(
        security=SecuritySettings(ipc_token="token", download_secret="download"),
    )
    config.file_transfer.output_dir = str(tmp_path / "out")
    config.file_transfer.public_base_url = "http://worker/images"
    store = SQLiteStore(tmp_path / "worker.sqlite3")
    processor = TaskProcessor(
        config=config,
        store=store,
        wx_client=FakeWxClient(
            [
                {
                    "type": "text",
                    "timestamp": 100,
                    "sender_id": "wxid_abc",
                    "sender_name": "张三",
                    "content": "今天方案不错",
                }
            ]
        ),
        llm_client=MockLLMClient(),
        image_client=PlaceholderImageGenClient(),
    )
    sent: list[SignalMessage] = []

    async def send(signal: SignalMessage) -> None:
        sent.append(signal)

    trigger = TriggerDetectedPayload(
        group_id="g",
        trigger_user="u",
        trigger_content="@总结",
        trigger_symbol="@",
        trigger_time=datetime(2026, 5, 23, tzinfo=UTC),
        since=datetime(2026, 5, 23, tzinfo=UTC),
        until=datetime(2026, 5, 23, 0, 10, tzinfo=UTC),
        time_range_mode="fixed_minutes",
        request_id="req-1",
    )
    result = await processor.process_trigger(trigger, send)
    assert result.type == MessageType.TASK_COMPLETED
    assert any(item.type == MessageType.TASK_ACCEPTED for item in sent)
    assert result.payload["image_file"]["download_url"].startswith("http://worker/images/")
    assert store.get_task("req-1")["status"] == "completed"


async def test_task_processor_blocks_sensitive_group_for_cloud_llm(tmp_path) -> None:
    config = WorkerConfig(security=SecuritySettings(ipc_token="token", download_secret="download"))
    config.privacy.sensitive_groups = ["g"]
    store = SQLiteStore(tmp_path / "worker.sqlite3")
    processor = TaskProcessor(
        config=config,
        store=store,
        wx_client=FakeWxClient(
            [{"type": "text", "timestamp": 100, "sender_id": "u", "content": "secret"}]
        ),
        llm_client=CloudLLMClient(),
        image_client=PlaceholderImageGenClient(),
    )
    sent: list[SignalMessage] = []

    async def send(signal: SignalMessage) -> None:
        sent.append(signal)

    trigger = TriggerDetectedPayload(
        group_id="g",
        trigger_user="u",
        trigger_content="@总结",
        trigger_symbol="@",
        trigger_time=datetime(2026, 5, 23, tzinfo=UTC),
        since=datetime(2026, 5, 23, tzinfo=UTC),
        until=datetime(2026, 5, 23, 0, 10, tzinfo=UTC),
        time_range_mode="fixed_minutes",
        request_id="req-sensitive",
    )
    result = await processor.process_trigger(trigger, send)
    assert result.type == MessageType.TASK_FAILED
    assert result.payload["error_code"] == "PRIVACY_BLOCKED"


async def test_task_processor_degrades_to_text_when_image_generation_fails(tmp_path) -> None:
    config = WorkerConfig(security=SecuritySettings(ipc_token="token", download_secret="download"))
    store = SQLiteStore(tmp_path / "worker.sqlite3")
    processor = TaskProcessor(
        config=config,
        store=store,
        wx_client=FakeWxClient(
            [{"type": "text", "timestamp": 100, "sender_id": "u", "content": "hello"}]
        ),
        llm_client=MockLLMClient(),
        image_client=BrokenImageClient(),
    )
    sent: list[SignalMessage] = []

    async def send(signal: SignalMessage) -> None:
        sent.append(signal)

    trigger = TriggerDetectedPayload(
        group_id="g",
        trigger_user="u",
        trigger_content="@总结",
        trigger_symbol="@",
        trigger_time=datetime(2026, 5, 23, tzinfo=UTC),
        since=datetime(2026, 5, 23, tzinfo=UTC),
        until=datetime(2026, 5, 23, 0, 10, tzinfo=UTC),
        time_range_mode="fixed_minutes",
        request_id="req-image-fail",
    )
    result = await processor.process_trigger(trigger, send)
    assert result.type == MessageType.TASK_COMPLETED
    assert result.payload["image_file"] is None
    assert "图片生成失败" in result.payload["summary_text"]

