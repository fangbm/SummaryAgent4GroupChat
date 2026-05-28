from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Literal
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator

SCHEMA_VERSION = "1.0"


def utc_now() -> datetime:
    return datetime.now(UTC)


class MessageType(StrEnum):
    TRIGGER_DETECTED = "TRIGGER_DETECTED"
    TASK_ACCEPTED = "TASK_ACCEPTED"
    PROGRESS_UPDATE = "PROGRESS_UPDATE"
    TASK_COMPLETED = "TASK_COMPLETED"
    TASK_FAILED = "TASK_FAILED"


class TaskStatus(StrEnum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    SENT = "sent"


class SignalMessage(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["1.0"] = "1.0"
    msg_id: str = Field(default_factory=lambda: str(uuid4()))
    timestamp: datetime = Field(default_factory=utc_now)
    type: MessageType
    payload: dict[str, Any] = Field(default_factory=dict)
    reply_to: str | None = None

    @field_validator("timestamp", mode="before")
    @classmethod
    def normalize_timestamp(cls, value: object) -> object:
        if isinstance(value, datetime) and value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value

    @classmethod
    def from_payload(
        cls,
        message_type: MessageType,
        payload: BaseModel | dict[str, Any],
        *,
        reply_to: str | None = None,
    ) -> SignalMessage:
        payload_dict = (
            payload.model_dump(mode="json") if isinstance(payload, BaseModel) else payload
        )
        return cls(type=message_type, payload=payload_dict, reply_to=reply_to)


class TriggerDetectedPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    group_id: str
    group_name: str | None = None
    trigger_user: str
    trigger_user_name: str | None = None
    trigger_content: str
    trigger_symbol: str
    trigger_time: datetime
    last_trigger_time: datetime | None = None
    since: datetime
    until: datetime
    time_range_mode: str
    request_id: str


class TaskAcceptedPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    request_id: str
    estimated_seconds: int = 30
    status: Literal["processing"] = "processing"


class ProgressUpdatePayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    request_id: str
    stage: str
    progress_percent: int = Field(ge=0, le=100)
    detail: str


class ImageFilePayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    filename: str
    size_bytes: int
    mime_type: str = "image/png"
    download_url: str
    checksum_sha256: str


class TaskCompletedPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    request_id: str
    group_id: str
    summary_text: str
    image_file: ImageFilePayload | None = None
    generated_at: datetime = Field(default_factory=utc_now)


class TaskFailedPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    request_id: str
    error_code: str
    error_message: str
    retryable: bool
