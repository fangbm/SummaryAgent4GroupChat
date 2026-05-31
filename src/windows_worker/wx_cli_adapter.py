from __future__ import annotations

import asyncio
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Protocol
from uuid import uuid4

from pipeline_core.errors import ErrorCode, PipelineError
from windows_worker.config import WxCliSettings


class WxCliAdapter(Protocol):
    async def export_chat_history(
        self,
        *,
        group_id: str,
        group_name: str | None,
        since: datetime,
        until: datetime,
    ) -> list[dict[str, Any]]:
        ...


class CliWxClient:
    def __init__(self, settings: WxCliSettings):
        self.settings = settings
        Path(settings.temp_dir).mkdir(parents=True, exist_ok=True)

    async def export_chat_history(
        self,
        *,
        group_id: str,
        group_name: str | None,
        since: datetime,
        until: datetime,
    ) -> list[dict[str, Any]]:
        chat_name = self.settings.group_name_map.get(group_id) or group_name or group_id
        output = Path(self.settings.temp_dir) / f"wx-export-{uuid4()}.json"
        cmd = self.build_export_command(chat_name, since, until, output)
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        _, stderr = await proc.communicate()
        if proc.returncode != 0:
            raise PipelineError(
                ErrorCode.WXDB_DECRYPT_FAILED,
                stderr.decode("utf-8", errors="replace") or "wxdb export failed",
                retryable=False,
            )
        try:
            data = json.loads(output.read_text(encoding="utf-8"))
        finally:
            output.unlink(missing_ok=True)
        messages = data.get("messages", data if isinstance(data, list) else [])
        if not isinstance(messages, list):
            raise PipelineError(ErrorCode.WXDB_DECRYPT_FAILED, "wxdb JSON format is invalid")
        return [self.normalize_message(msg) for msg in messages if isinstance(msg, dict)]

    def build_export_command(
        self,
        chat_name: str,
        since: datetime,
        until: datetime,
        output: Path,
    ) -> list[str]:
        # The legacy external history command parses local time with a space separator.
        cmd = [
            self.settings.executable,
            "export",
            chat_name,
            "--since",
            since.strftime("%Y-%m-%d %H:%M:%S"),
            "--until",
            until.strftime("%Y-%m-%d %H:%M:%S"),
            "--format",
            self.settings.export_format,
            "-o",
            str(output),
            "-n",
            str(self.settings.max_messages),
        ]
        return cmd

    @staticmethod
    def normalize_message(message: dict[str, Any]) -> dict[str, Any]:
        normalized = dict(message)
        normalized.setdefault("sender_name", message.get("sender"))
        normalized.setdefault("sender_id", message.get("sender_username") or message.get("sender"))
        normalized.setdefault("type", message.get("type", "text"))
        return normalized


class FakeWxClient:
    def __init__(self, messages: list[dict[str, Any]]):
        self.messages = messages

    async def export_chat_history(
        self,
        *,
        group_id: str,
        group_name: str | None,
        since: datetime,
        until: datetime,
    ) -> list[dict[str, Any]]:
        return list(self.messages)
