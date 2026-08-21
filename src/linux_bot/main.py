from __future__ import annotations

import argparse
import asyncio
import logging
import os
from collections.abc import Awaitable, Callable

from linux_bot.config import load_bot_config
from linux_bot.ipc_client import WindowsIpcClient
from linux_bot.service import LinuxBotService
from linux_bot.wx_bot_adapter import CliWxBotAdapter
from pipeline_core.logging import configure_logging
from pipeline_core.protocol import SignalMessage
from pipeline_core.storage import SQLiteStore

logger = logging.getLogger(__name__)


async def supervise_receiver(
    ipc: WindowsIpcClient,
    handler: Callable[[SignalMessage], Awaitable[None]],
    reconnect_seconds: int,
) -> None:
    """Keep the IPC receive loop alive: reconnect and restart on any exit."""
    while True:
        try:
            await ipc.receive_loop(handler)
            logger.warning("IPC receive loop ended; reconnecting in %ss", reconnect_seconds)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("IPC receive loop crashed; reconnecting in %ss", reconnect_seconds)
        await asyncio.sleep(max(reconnect_seconds, 1))
        try:
            await ipc.connect()
        except Exception:
            logger.exception("IPC reconnect failed; will retry")


async def run(config_path: str) -> None:
    config = load_bot_config(config_path)
    configure_logging(config.runtime.log_level)
    store = SQLiteStore(config.storage.sqlite_path)
    adapter = CliWxBotAdapter(config.bot.cli_path)
    ipc = WindowsIpcClient(config.windows_bridge.websocket_url, config.windows_bridge.token)
    service = LinuxBotService(
        config=config,
        store=store,
        adapter=adapter,
        send_signal=ipc.send,
    )
    await ipc.connect()
    receiver = asyncio.create_task(
        supervise_receiver(
            ipc,
            service.handle_worker_signal,
            config.windows_bridge.reconnect_seconds,
        )
    )
    try:
        async for message in adapter.iter_messages():
            await service.handle_incoming_message(message)
    finally:
        receiver.cancel()
        try:
            await receiver
        except asyncio.CancelledError:
            pass
        await ipc.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default=os.getenv("LINUX_BOT_CONFIG", "config/bot.yaml"))
    args = parser.parse_args()
    asyncio.run(run(args.config))


if __name__ == "__main__":
    main()

