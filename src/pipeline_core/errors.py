from __future__ import annotations

from enum import StrEnum


class ErrorCode(StrEnum):
    WX_CLI_DECRYPT_FAILED = "WX_CLI_DECRYPT_FAILED"
    WX_CLI_NO_HISTORY = "WX_CLI_NO_HISTORY"
    LLM_TIMEOUT = "LLM_TIMEOUT"
    LLM_RATE_LIMIT = "LLM_RATE_LIMIT"
    IMAGE_GEN_FAILED = "IMAGE_GEN_FAILED"
    IPC_DISCONNECTED = "IPC_DISCONNECTED"
    FILE_TRANSFER_FAILED = "FILE_TRANSFER_FAILED"
    PRIVACY_BLOCKED = "PRIVACY_BLOCKED"
    CONFIG_INVALID = "CONFIG_INVALID"
    UNKNOWN = "UNKNOWN"


RETRYABLE_ERRORS: set[ErrorCode] = {
    ErrorCode.LLM_TIMEOUT,
    ErrorCode.LLM_RATE_LIMIT,
    ErrorCode.IMAGE_GEN_FAILED,
    ErrorCode.IPC_DISCONNECTED,
    ErrorCode.FILE_TRANSFER_FAILED,
}


class PipelineError(Exception):
    def __init__(self, code: ErrorCode, message: str, *, retryable: bool | None = None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.retryable = code in RETRYABLE_ERRORS if retryable is None else retryable

    def as_payload(self) -> dict[str, object]:
        return {
            "error_code": self.code.value,
            "error_message": self.message,
            "retryable": self.retryable,
        }


class PrivacyBlockedError(PipelineError):
    def __init__(self, message: str):
        super().__init__(ErrorCode.PRIVACY_BLOCKED, message, retryable=False)

