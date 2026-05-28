from __future__ import annotations

import re
from dataclasses import dataclass

from windows_worker.config import WechatListenSettings
from windows_worker.wechat_adapter import WechatMessage


@dataclass(frozen=True)
class TriggerMatch:
    group_id: str
    group_name: str | None
    sender_id: str
    sender_name: str | None
    trigger_content: str
    trigger_symbol: str
    timestamp: int


class WindowsTriggerMatcher:
    def __init__(self, settings: WechatListenSettings):
        self.settings = settings
        self._regexes = (
            [re.compile(pattern) for pattern in settings.triggers]
            if settings.match_mode == "regex"
            else []
        )

    def match(self, message: WechatMessage) -> TriggerMatch | None:
        if not self._group_allowed(message.group_id, message.group_name):
            return None
        if message.sender_id in self.settings.blacklist_users:
            return None
        if message.is_self and self.settings.ignore_self:
            return None
        if message.type not in self.settings.content_types:
            return None
        symbol = self._match_symbol(message.content)
        if symbol is None:
            return None
        return TriggerMatch(
            group_id=message.group_id,
            group_name=message.group_name,
            sender_id=message.sender_id,
            sender_name=message.sender_name,
            trigger_content=message.content,
            trigger_symbol=symbol,
            timestamp=message.timestamp,
        )

    def _group_allowed(self, group_id: str, group_name: str | None) -> bool:
        if not self.settings.whitelist_groups:
            return True
        name = group_name or ""
        return any(
            item == group_id or (name and item in name)
            for item in self.settings.whitelist_groups
        )

    def _match_symbol(self, content: str) -> str | None:
        if self.settings.match_mode == "prefix":
            return next(
                (symbol for symbol in self.settings.triggers if content.startswith(symbol)),
                None,
            )
        if self.settings.match_mode == "contains":
            return next((symbol for symbol in self.settings.triggers if symbol in content), None)
        for regex in self._regexes:
            found = regex.search(content)
            if found:
                return found.group(0)
        return None
