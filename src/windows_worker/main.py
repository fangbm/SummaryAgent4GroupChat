from __future__ import annotations

import argparse
import os

import uvicorn

from pipeline_core.logging import configure_logging
from pipeline_core.storage import SQLiteStore
from windows_worker.config import load_worker_config
from windows_worker.ipc_server import create_app
from windows_worker.providers.image_gen import build_image_client
from windows_worker.providers.llm import build_llm_client
from windows_worker.single_host_service import WindowsSingleHostService
from windows_worker.task_processor import TaskProcessor
from windows_worker.wechat_adapter import build_wechat_adapter
from windows_worker.wx_cli_adapter import CliWxClient


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config",
        default=os.getenv("WINDOWS_WORKER_CONFIG", "config/worker.yaml"),
    )
    parser.add_argument(
        "--mode",
        choices=["single", "api"],
        default="single",
        help="single runs the Windows-only WeChat pipeline; api only exposes IPC/HTTP server.",
    )
    args = parser.parse_args()
    config = load_worker_config(args.config)
    configure_logging("INFO")
    store = SQLiteStore(config.storage.sqlite_path)
    processor = TaskProcessor(
        config=config,
        store=store,
        wx_client=CliWxClient(config.wx_cli),
        llm_client=build_llm_client(config.llm),
        image_client=build_image_client(config.image_gen),
    )
    if args.mode == "api":
        app = create_app(config, processor)
        uvicorn.run(app, host=config.server.host, port=config.server.port)
        return

    service = WindowsSingleHostService(
        config=config,
        store=store,
        wechat=build_wechat_adapter(config.wechat),
        processor=processor,
    )
    import asyncio

    asyncio.run(service.run_forever())


if __name__ == "__main__":
    main()
