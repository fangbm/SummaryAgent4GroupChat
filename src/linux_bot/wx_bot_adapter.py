from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Protocol

from pipeline_core.errors import ErrorCode, PipelineError


class WxBotAdapter(Protocol):
    def iter_messages(self) -> AsyncIterator[dict[str, object]]:
        ...

    async def send_image(self, group_id: str, image_path: str | Path) -> None:
        ...

    async def send_text(self, group_id: str, text: str) -> None:
        ...


class CliWxBotAdapter:
    def __init__(self, cli_path: str):
        self.cli_path = cli_path

    async def iter_messages(self) -> AsyncIterator[dict[str, object]]:
        proc = await asyncio.create_subprocess_exec(
            self.cli_path,
            "watch",
            "--json",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        assert proc.stdout is not None
        async for raw in proc.stdout:
            line = raw.decode("utf-8", errors="replace").strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue

    async def send_image(self, group_id: str, image_path: str | Path) -> None:
        await self._run("send-image", group_id, str(image_path))

    async def send_text(self, group_id: str, text: str) -> None:
        await self._run("send-text", group_id, text)

    async def _run(self, *args: str) -> None:
        proc = await asyncio.create_subprocess_exec(
            self.cli_path,
            *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        _, stderr = await proc.communicate()
        if proc.returncode != 0:
            raise PipelineError(
                ErrorCode.UNKNOWN,
                stderr.decode("utf-8", errors="replace") or "wx-bot-cli command failed",
            )


class FakeWxBotAdapter:
    def __init__(self, messages: list[dict[str, object]] | None = None):
        self.messages = messages or []
        self.sent_images: list[tuple[str, str]] = []
        self.sent_texts: list[tuple[str, str]] = []

    async def iter_messages(self) -> AsyncIterator[dict[str, object]]:
        for message in self.messages:
            yield message

    async def send_image(self, group_id: str, image_path: str | Path) -> None:
        self.sent_images.append((group_id, str(image_path)))

    async def send_text(self, group_id: str, text: str) -> None:
        self.sent_texts.append((group_id, text))
