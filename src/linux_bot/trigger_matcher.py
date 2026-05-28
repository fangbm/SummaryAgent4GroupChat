from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from linux_bot.config import ListenSettings, MatchMode, MessageSettings


@dataclass(frozen=True)
class TriggerMatch:
    trigger_symbol: str
    trigger_content: str
    group_id: str
    group_name: str | None
    sender_id: str
    sender_name: str | None
    timestamp: int


class TriggerMatcher:
    def __init__(self, listen: ListenSettings, message: MessageSettings):
        self.listen = listen
        self.message = message
        self._regexes = (
            [re.compile(pattern) for pattern in listen.triggers]
            if listen.match_mode == MatchMode.REGEX
            else []
        )

    def match(self, msg: dict[str, Any]) -> TriggerMatch | None:
        group_id = str(msg.get("group_id") or "")
        group_name = msg.get("group_name")
        sender_id = str(msg.get("sender_id") or "")
        if not self._group_allowed(group_id, group_name):
            return None
        if sender_id in self.listen.blacklist_users:
            return None
        if msg.get("is_self") and self.message.ignore_self:
            return None
        if msg.get("type") not in self.message.content_types:
            return None
        content = str(msg.get("content") or "")
        symbol = self._match_symbol(content)
        if symbol is None:
            return None
        return TriggerMatch(
            trigger_symbol=symbol,
            trigger_content=content,
            group_id=group_id,
            group_name=str(group_name) if group_name else None,
            sender_id=sender_id,
            sender_name=str(msg.get("sender_name")) if msg.get("sender_name") else None,
            timestamp=int(msg.get("timestamp") or 0),
        )

    def _group_allowed(self, group_id: str, group_name: object) -> bool:
        if not self.listen.whitelist_groups:
            return True
        group_name_text = str(group_name or "")
        return any(
            item == group_id or (group_name_text and item in group_name_text)
            for item in self.listen.whitelist_groups
        )

    def _match_symbol(self, content: str) -> str | None:
        if self.listen.match_mode == MatchMode.PREFIX:
            return next(
                (symbol for symbol in self.listen.triggers if content.startswith(symbol)),
                None,
            )
        if self.listen.match_mode == MatchMode.CONTAINS:
            return next((symbol for symbol in self.listen.triggers if symbol in content), None)
        for pattern in self._regexes:
            found = pattern.search(content)
            if found:
                return found.group(0)
        return None
