from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

from pipeline_core.protocol import (
    MessageType,
    SignalMessage,
    TaskCompletedPayload,
    TaskFailedPayload,
    TaskStatus,
    TriggerDetectedPayload,
)
from pipeline_core.storage import SQLiteStore
from windows_worker.config import WechatTimeRangeSettings, WorkerConfig
from windows_worker.task_processor import TaskProcessor
from windows_worker.trigger import WindowsTriggerMatcher
from windows_worker.wechat_adapter import WechatMessage, WindowsWechatAdapter


class WindowsTimeRangeManager:
    def __init__(self, store: SQLiteStore, settings: WechatTimeRangeSettings):
        self.store = store
        self.settings = settings

    def on_trigger(
        self,
        group_id: str,
        current_ts: int,
    ) -> tuple[datetime, datetime, str, datetime | None]:
        until = datetime.fromtimestamp(current_ts, tz=UTC)
        last_ts = self.store.get_last_trigger(group_id)
        last_dt = datetime.fromtimestamp(last_ts, tz=UTC) if last_ts else None
        if self.settings.mode == "fixed_minutes":
            since = until - timedelta(minutes=self.settings.fixed_minutes)
            mode = "fixed_minutes"
        elif self.settings.mode == "fixed_hours":
            since = until - timedelta(hours=self.settings.fixed_hours)
            mode = "fixed_hours"
        elif self.settings.mode == "today":
            # "Today" follows the host's local calendar, not UTC midnight.
            local_until = until.astimezone()
            since = local_until.replace(
                hour=0, minute=0, second=0, microsecond=0
            ).astimezone(UTC)
            mode = "today"
        elif last_dt:
            since = last_dt
            mode = "between_triggers"
        else:
            since = until - timedelta(minutes=self.settings.fallback_minutes)
            mode = "default_fallback"
        self.store.set_last_trigger(group_id, current_ts)
        return since, until, mode, last_dt


class WindowsSingleHostService:
    def __init__(
        self,
        *,
        config: WorkerConfig,
        store: SQLiteStore,
        wechat: WindowsWechatAdapter,
        processor: TaskProcessor,
    ):
        self.config = config
        self.store = store
        self.wechat = wechat
        self.processor = processor
        self.matcher = WindowsTriggerMatcher(config.wechat.listen)
        self.time_ranges = WindowsTimeRangeManager(store, config.wechat.time_range)

    async def run_forever(self) -> None:
        async for message in self.wechat.iter_messages():
            await self.handle_message(message)

    async def handle_message(self, message: WechatMessage) -> SignalMessage | None:
        match = self.matcher.match(message)
        if match is None:
            return None
        since, until, mode, last_trigger_time = self.time_ranges.on_trigger(
            match.group_id,
            match.timestamp,
        )
        request_id = f"req-{uuid4()}"
        trigger = TriggerDetectedPayload(
            group_id=match.group_id,
            group_name=match.group_name,
            trigger_user=match.sender_id,
            trigger_user_name=match.sender_name,
            trigger_content=match.trigger_content,
            trigger_symbol=match.trigger_symbol,
            trigger_time=until,
            last_trigger_time=last_trigger_time,
            since=since,
            until=until,
            time_range_mode=mode,
            request_id=request_id,
        )
        self.store.upsert_task(
            request_id,
            status=TaskStatus.PENDING.value,
            group_id=match.group_id,
            payload=trigger.model_dump(mode="json"),
        )

        async def on_signal(signal: SignalMessage) -> None:
            if signal.type == MessageType.TASK_COMPLETED:
                await self._send_completed(TaskCompletedPayload.model_validate(signal.payload))
            elif signal.type == MessageType.TASK_FAILED:
                await self._send_failed(
                    TaskFailedPayload.model_validate(signal.payload),
                    match.group_id,
                )

        return await self.processor.process_trigger(trigger, on_signal)

    async def _send_completed(self, payload: TaskCompletedPayload) -> None:
        receiver = self._receiver(payload.group_id)
        if payload.image_file:
            image_path = Path(self.config.file_transfer.output_dir) / payload.image_file.filename
            await self.wechat.send_image(receiver, image_path)
        else:
            await self.wechat.send_text(receiver, payload.summary_text)
        self.store.update_task(payload.request_id, status=TaskStatus.SENT.value)

    async def _send_failed(self, payload: TaskFailedPayload, group_id: str) -> None:
        receiver = self._receiver(group_id)
        await self.wechat.send_text(receiver, f"总结失败：{payload.error_message}")
        self.store.update_task(payload.request_id, status=TaskStatus.FAILED.value)

    def _receiver(self, group_id: str) -> str:
        return self.config.wechat.receiver_map.get(group_id, group_id)
