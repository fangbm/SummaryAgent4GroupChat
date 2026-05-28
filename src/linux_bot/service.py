from __future__ import annotations

from collections.abc import Awaitable, Callable
from uuid import uuid4

from linux_bot.config import LinuxBotConfig
from linux_bot.file_downloader import download_image
from linux_bot.state_manager import GroupStateManager
from linux_bot.trigger_matcher import TriggerMatcher
from linux_bot.wx_bot_adapter import WxBotAdapter
from pipeline_core.protocol import (
    MessageType,
    ProgressUpdatePayload,
    SignalMessage,
    TaskAcceptedPayload,
    TaskCompletedPayload,
    TaskFailedPayload,
    TaskStatus,
    TriggerDetectedPayload,
)
from pipeline_core.storage import SQLiteStore


class LinuxBotService:
    def __init__(
        self,
        *,
        config: LinuxBotConfig,
        store: SQLiteStore,
        adapter: WxBotAdapter,
        send_signal: Callable[[SignalMessage], Awaitable[None]],
    ):
        self.config = config
        self.store = store
        self.adapter = adapter
        self.send_signal = send_signal
        self.matcher = TriggerMatcher(config.bot.listen, config.bot.message)
        self.state = GroupStateManager(store, config.bot.time_range)

    async def handle_incoming_message(self, raw_message: dict[str, object]) -> SignalMessage | None:
        match = self.matcher.match(raw_message)
        if match is None:
            return None
        time_range = self.state.on_trigger(match.group_id, match.timestamp)
        request_id = f"req-{uuid4()}"
        payload = TriggerDetectedPayload(
            group_id=match.group_id,
            group_name=match.group_name,
            trigger_user=match.sender_id,
            trigger_user_name=match.sender_name,
            trigger_content=match.trigger_content,
            trigger_symbol=match.trigger_symbol,
            trigger_time=time_range.until,
            last_trigger_time=time_range.last_trigger_time,
            since=time_range.since,
            until=time_range.until,
            time_range_mode=time_range.mode,
            request_id=request_id,
        )
        self.store.upsert_task(
            request_id,
            status=TaskStatus.PENDING.value,
            group_id=match.group_id,
            payload=payload.model_dump(mode="json"),
        )
        signal = SignalMessage.from_payload(MessageType.TRIGGER_DETECTED, payload)
        await self.send_signal(signal)
        return signal

    async def handle_worker_signal(self, signal: SignalMessage) -> None:
        if signal.type == MessageType.TASK_COMPLETED:
            payload = TaskCompletedPayload.model_validate(signal.payload)
            if payload.image_file:
                image_path = await download_image(
                    payload.image_file.download_url,
                    self.config.windows_bridge.file_transfer.download_dir,
                    filename=payload.image_file.filename,
                    timeout_seconds=self.config.windows_bridge.file_transfer.timeout_seconds,
                    checksum_sha256=payload.image_file.checksum_sha256,
                )
                await self.adapter.send_image(payload.group_id, image_path)
            else:
                await self.adapter.send_text(payload.group_id, payload.summary_text)
            self.store.update_task(payload.request_id, status=TaskStatus.SENT.value)
        elif signal.type == MessageType.TASK_ACCEPTED:
            accepted_payload = TaskAcceptedPayload.model_validate(signal.payload)
            self.store.update_task(
                accepted_payload.request_id,
                status=TaskStatus.PROCESSING.value,
            )
        elif signal.type == MessageType.PROGRESS_UPDATE:
            progress_payload = ProgressUpdatePayload.model_validate(signal.payload)
            self.store.update_task(
                progress_payload.request_id,
                status=TaskStatus.PROCESSING.value,
            )
        elif signal.type == MessageType.TASK_FAILED:
            failed_payload = TaskFailedPayload.model_validate(signal.payload)
            self.store.update_task(
                failed_payload.request_id,
                status=TaskStatus.FAILED.value,
                error_code=failed_payload.error_code,
                error_message=failed_payload.error_message,
            )
