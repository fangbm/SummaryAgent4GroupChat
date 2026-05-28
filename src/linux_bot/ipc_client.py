from __future__ import annotations

import json
from collections.abc import Awaitable, Callable
from typing import Any

import websockets

from pipeline_core.auth import authorization_header
from pipeline_core.protocol import SignalMessage


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
        if self._connection is None:
            await self.connect()
        assert self._connection is not None
        async for raw in self._connection:
            payload = json.loads(raw)
            await handler(SignalMessage.model_validate(payload))
