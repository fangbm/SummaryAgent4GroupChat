from __future__ import annotations

import json
import logging
from collections.abc import Awaitable, Callable
from typing import Any

import websockets

from pipeline_core.auth import authorization_header
from pipeline_core.protocol import SignalMessage

logger = logging.getLogger(__name__)


class WindowsIpcClient:
    def __init__(self, url: str, token: str):
        self.url = url
        self.token = token
        self._connection: Any | None = None

    async def connect(self) -> None:
        headers = [("Authorization", authorization_header(self.token))]
        try:
            self._connection = await websockets.connect(self.url, extra_headers=headers)
        except TypeError:
            self._connection = await websockets.connect(self.url, additional_headers=headers)

    async def close(self) -> None:
        if self._connection is not None:
            await self._connection.close()
            self._connection = None

    async def send(self, signal: SignalMessage) -> None:
        if self._connection is None:
            await self.connect()
        assert self._connection is not None
        await self._connection.send(signal.model_dump_json())

    async def receive_loop(self, handler: Callable[[SignalMessage], Awaitable[None]]) -> None:
        """Consume worker signals until the connection closes.

        Malformed frames and handler failures are logged and skipped so one bad
        message cannot silently terminate the whole bot.
        """
        if self._connection is None:
            await self.connect()
        assert self._connection is not None
        async for raw in self._connection:
            try:
                payload = json.loads(raw)
                signal = SignalMessage.model_validate(payload)
            except Exception:
                preview = raw if isinstance(raw, str) else str(raw)
                logger.warning("dropping malformed IPC frame: %.200s", preview)
                continue
            try:
                await handler(signal)
            except Exception:
                logger.exception(
                    "worker signal handler failed for type=%s request=%s",
                    signal.type,
                    getattr(signal.payload, "get", lambda *_: None)("request_id"),
                )
