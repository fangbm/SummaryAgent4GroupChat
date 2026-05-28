from __future__ import annotations

import re
from collections.abc import Iterable
from copy import deepcopy
from enum import StrEnum
from typing import Any

from pipeline_core.errors import PrivacyBlockedError


class PrivacyMode(StrEnum):
    PROTECTED = "protected"
    CLOUD = "cloud"
    LOCAL_ONLY = "local_only"


DEFAULT_PATTERNS: tuple[tuple[str, str], ...] = (
    (r"wxid_[A-Za-z0-9_-]+", "wxid_***"),
    (r"\b1[3-9]\d{9}\b", "手机号***"),
    (r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", "邮箱***"),
)


class PrivacyGuard:
    def __init__(
        self,
        *,
        mode: str = PrivacyMode.PROTECTED,
        redact_enabled: bool = True,
        max_messages: int = 800,
        max_chars: int = 20_000,
        cloud_allowed: bool = True,
        sensitive_groups: Iterable[str] = (),
    ):
        self.mode = PrivacyMode(mode)
        self.redact_enabled = redact_enabled
        self.max_messages = max_messages
        self.max_chars = max_chars
        self.cloud_allowed = cloud_allowed
        self.sensitive_groups = set(sensitive_groups)

    def ensure_cloud_allowed(self, group_id: str, *, llm_is_local: bool) -> None:
        if llm_is_local:
            return
        if self.mode == PrivacyMode.LOCAL_ONLY or not self.cloud_allowed:
            raise PrivacyBlockedError("Cloud LLM is disabled by privacy policy")
        if group_id in self.sensitive_groups:
            raise PrivacyBlockedError(f"Group {group_id} is configured as local-only")

    def redact_text(self, text: str) -> str:
        if not self.redact_enabled:
            return text
        redacted = text
        for pattern, replacement in DEFAULT_PATTERNS:
            redacted = re.sub(pattern, replacement, redacted)
        return redacted

    def prepare_messages(self, messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
        selected = deepcopy(messages[-self.max_messages :])
        for msg in selected:
            for key in ("sender_id", "sender_name", "content"):
                value = msg.get(key)
                if isinstance(value, str):
                    msg[key] = self.redact_text(value)
        return selected

    def enforce_text_budget(self, text: str) -> str:
        if len(text) <= self.max_chars:
            return text
        return text[: self.max_chars] + "\n...[内容已按隐私预算截断]"

