from __future__ import annotations

import argparse
import asyncio
import os

from linux_bot.config import load_bot_config
from linux_bot.ipc_client import WindowsIpcClient
from linux_bot.service import LinuxBotService
from linux_bot.wx_bot_adapter import CliWxBotAdapter
from pipeline_core.logging import configure_logging
from pipeline_core.storage import SQLiteStore


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
    receiver = asyncio.create_task(ipc.receive_loop(service.handle_worker_signal))
    try:
        async for message in adapter.iter_messages():
            await service.handle_incoming_message(message)
    finally:
        receiver.cancel()
        await ipc.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default=os.getenv("LINUX_BOT_CONFIG", "config/bot.yaml"))
    args = parser.parse_args()
    asyncio.run(run(args.config))


if __name__ == "__main__":
    main()

