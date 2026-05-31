from __future__ import annotations

import time
from collections.abc import Awaitable, Callable
from pathlib import Path

from pipeline_core.auth import sign_image_url
from pipeline_core.errors import ErrorCode, PipelineError
from pipeline_core.metrics import TASK_DURATION_SECONDS, TASK_ERRORS_TOTAL, TASKS_TOTAL
from pipeline_core.privacy import PrivacyGuard
from pipeline_core.protocol import (
    ImageFilePayload,
    MessageType,
    ProgressUpdatePayload,
    SignalMessage,
    TaskAcceptedPayload,
    TaskCompletedPayload,
    TaskFailedPayload,
    TaskStatus,
    TriggerDetectedPayload,
)
from pipeline_core.retry import RetryPolicy, retry_async
from pipeline_core.storage import SQLiteStore
from windows_worker.chat_formatter import ChatFormatter
from windows_worker.config import WorkerConfig
from windows_worker.providers.image_gen import ImageGenClient
from windows_worker.providers.llm import LLMClient
from windows_worker.wx_cli_adapter import WxCliAdapter

ProgressSender = Callable[[SignalMessage], Awaitable[None]]


class TaskProcessor:
    def __init__(
        self,
        *,
        config: WorkerConfig,
        store: SQLiteStore,
        wx_client: WxCliAdapter,
        llm_client: LLMClient,
        image_client: ImageGenClient,
    ):
        self.config = config
        self.store = store
        self.wx_client = wx_client
        self.llm_client = llm_client
        self.image_client = image_client
        self.privacy = PrivacyGuard(
            mode=config.privacy.mode,
            redact_enabled=config.privacy.redact_enabled,
            max_messages=config.privacy.max_messages_to_llm,
            max_chars=config.privacy.max_chars_to_llm,
            cloud_allowed=config.privacy.cloud_allowed,
            sensitive_groups=config.privacy.sensitive_groups,
        )

    async def process_signal(
        self,
        signal: SignalMessage,
        send: ProgressSender,
    ) -> SignalMessage:
        if signal.type != MessageType.TRIGGER_DETECTED:
            raise PipelineError(ErrorCode.UNKNOWN, f"Unsupported message type: {signal.type}")
        trigger = TriggerDetectedPayload.model_validate(signal.payload)
        return await self.process_trigger(trigger, send, reply_to=signal.msg_id)

    async def process_trigger(
        self,
        trigger: TriggerDetectedPayload,
        send: ProgressSender,
        *,
        reply_to: str | None = None,
    ) -> SignalMessage:
        start = time.perf_counter()
        existing = self.store.get_task(trigger.request_id)
        if existing and existing["status"] == TaskStatus.COMPLETED.value:
            completed = self._completed_from_existing(existing, trigger)
            await send(completed)
            return completed

        self.store.upsert_task(
            trigger.request_id,
            status=TaskStatus.PROCESSING.value,
            group_id=trigger.group_id,
            payload=trigger.model_dump(mode="json"),
        )
        accepted = SignalMessage.from_payload(
            MessageType.TASK_ACCEPTED,
            TaskAcceptedPayload(request_id=trigger.request_id),
            reply_to=reply_to,
        )
        await send(accepted)

        try:
            await self._progress(send, trigger.request_id, "wx_export", 15, "正在导出聊天记录")
            messages = await self.wx_client.export_chat_history(
                group_id=trigger.group_id,
                group_name=trigger.group_name,
                since=trigger.since,
                until=trigger.until,
            )
            if not messages:
                raise PipelineError(
                    ErrorCode.WXDB_NO_HISTORY,
                    "该时间段无聊天记录",
                    retryable=False,
                )

            self.privacy.ensure_cloud_allowed(
                trigger.group_id,
                llm_is_local=self.llm_client.is_local,
            )
            protected = self.privacy.prepare_messages(messages)
            merged_input, _, _ = ChatFormatter.format_and_stats(protected)
            merged_input = self.privacy.enforce_text_budget(merged_input)

            await self._progress(send, trigger.request_id, "llm_processing", 50, "正在调用LLM整理")
            summary = await retry_async(
                lambda: self.llm_client.summarize(merged_input),
                policy=RetryPolicy(attempts=3, base_delay_seconds=1, max_delay_seconds=10),
                retryable=self._is_retryable,
            )

            image_payload: ImageFilePayload | None = None
            if self.config.image_gen.enabled:
                try:
                    await self._progress(
                        send,
                        trigger.request_id,
                        "image_generation",
                        75,
                        "正在生成图片",
                    )
                    filename = f"summary-{trigger.request_id}.png"
                    output_path = Path(self.config.file_transfer.output_dir) / filename
                    generated = await retry_async(
                        lambda: self.image_client.generate(summary, output_path),
                        policy=RetryPolicy(attempts=2, base_delay_seconds=1, max_delay_seconds=5),
                        retryable=self._is_retryable,
                    )
                    image_payload = ImageFilePayload(
                        filename=filename,
                        size_bytes=generated.size_bytes,
                        mime_type="image/png",
                        download_url=sign_image_url(
                            self.config.file_transfer.public_base_url,
                            filename,
                            self.config.security.download_secret,
                            self.config.security.download_url_ttl_seconds,
                        ),
                        checksum_sha256=generated.sha256,
                    )
                except Exception as exc:
                    if TASK_ERRORS_TOTAL is not None:
                        TASK_ERRORS_TOTAL.labels(code=ErrorCode.IMAGE_GEN_FAILED.value).inc()
                    summary += f"\n\n[图片生成失败，已降级为文字摘要：{exc}]"

            completed_payload = TaskCompletedPayload(
                request_id=trigger.request_id,
                group_id=trigger.group_id,
                summary_text=summary,
                image_file=image_payload,
            )
            self.store.update_task(
                trigger.request_id,
                status=TaskStatus.COMPLETED.value,
                summary_text=summary,
                image_filename=image_payload.filename if image_payload else None,
            )
            if TASKS_TOTAL is not None:
                TASKS_TOTAL.labels(status=TaskStatus.COMPLETED.value).inc()
            completed = SignalMessage.from_payload(
                MessageType.TASK_COMPLETED,
                completed_payload,
                reply_to=reply_to,
            )
            await send(completed)
            return completed
        except PipelineError as exc:
            failed = await self._fail(trigger.request_id, exc, send, reply_to)
            return failed
        except Exception as exc:
            failed = await self._fail(
                trigger.request_id,
                PipelineError(ErrorCode.UNKNOWN, str(exc), retryable=False),
                send,
                reply_to,
            )
            return failed
        finally:
            if TASK_DURATION_SECONDS is not None:
                TASK_DURATION_SECONDS.observe(time.perf_counter() - start)

    async def _progress(
        self,
        send: ProgressSender,
        request_id: str,
        stage: str,
        percent: int,
        detail: str,
    ) -> None:
        await send(
            SignalMessage.from_payload(
                MessageType.PROGRESS_UPDATE,
                ProgressUpdatePayload(
                    request_id=request_id,
                    stage=stage,
                    progress_percent=percent,
                    detail=detail,
                ),
            )
        )

    async def _fail(
        self,
        request_id: str,
        exc: PipelineError,
        send: ProgressSender,
        reply_to: str | None,
    ) -> SignalMessage:
        self.store.update_task(
            request_id,
            status=TaskStatus.FAILED.value,
            error_code=exc.code.value,
            error_message=exc.message,
        )
        if TASKS_TOTAL is not None:
            TASKS_TOTAL.labels(status=TaskStatus.FAILED.value).inc()
        if TASK_ERRORS_TOTAL is not None:
            TASK_ERRORS_TOTAL.labels(code=exc.code.value).inc()
        failed = SignalMessage.from_payload(
            MessageType.TASK_FAILED,
            TaskFailedPayload(
                request_id=request_id,
                error_code=exc.code.value,
                error_message=exc.message,
                retryable=exc.retryable,
            ),
            reply_to=reply_to,
        )
        await send(failed)
        return failed

    def _completed_from_existing(
        self,
        existing: dict[str, object],
        trigger: TriggerDetectedPayload,
    ) -> SignalMessage:
        payload = TaskCompletedPayload(
            request_id=trigger.request_id,
            group_id=trigger.group_id,
            summary_text=str(existing.get("summary_text") or ""),
            image_file=None,
        )
        return SignalMessage.from_payload(MessageType.TASK_COMPLETED, payload)

    @staticmethod
    def _is_retryable(exc: BaseException) -> bool:
        return isinstance(exc, PipelineError) and exc.retryable
