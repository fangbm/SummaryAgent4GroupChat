from __future__ import annotations

import asyncio
import os
import platform
import shutil
import subprocess
import time
from collections.abc import AsyncIterator
from dataclasses import dataclass
from importlib import import_module
from pathlib import Path
from queue import Empty, Queue
from threading import Thread
from typing import Any, NoReturn, Protocol

from pipeline_core.errors import ErrorCode, PipelineError
from windows_worker.config import WechatSettings


@dataclass(frozen=True)
class WechatMessage:
    group_id: str
    group_name: str | None
    sender_id: str
    sender_name: str | None
    content: str
    type: str
    timestamp: int
    is_self: bool = False


class WindowsWechatAdapter(Protocol):
    def iter_messages(self) -> AsyncIterator[WechatMessage]:
        ...

    async def send_text(self, receiver: str, text: str) -> None:
        ...

    async def send_image(self, receiver: str, image_path: str | Path) -> None:
        ...


class WxhookWechatAdapter:
    def __init__(self, settings: WechatSettings):
        self.settings = settings
        if platform.system() != "Windows":
            raise PipelineError(
                ErrorCode.CONFIG_INVALID,
                "wxhook provider only runs on Windows.",
                retryable=False,
            )
        try:
            wxhook = import_module("wxhook")
            self.events = import_module("wxhook.events")
            wxhook_utils = import_module("wxhook.utils")
        except ImportError as exc:  # pragma: no cover - only exercised on Windows with wxhook
            raise PipelineError(
                ErrorCode.CONFIG_INVALID,
                "wxhook is not installed in the current Python environment. "
                'Install with: python -m pip install -e ".[windows]"',
                retryable=False,
            ) from exc
        try:
            self._prepare_wxhook_tools(wxhook_utils)
        except OSError as exc:  # pragma: no cover - depends on local Windows file locks
            raise PipelineError(
                ErrorCode.CONFIG_INVALID,
                (
                    "wxhook tool files could not be prepared. If wxhook is already "
                    f"running, stop it and retry. Details: {exc}"
                ),
                retryable=False,
            ) from exc

        try:
            self.bot = wxhook.Bot(faked_version=settings.faked_version)
        except Exception as exc:
            raise PipelineError(
                ErrorCode.CONFIG_INVALID,
                f"wxhook Bot initialization failed: {exc}",
                retryable=False,
            ) from exc

        if settings.require_login and not self._check_login():
            raise PipelineError(
                ErrorCode.CONFIG_INVALID,
                "Windows WeChat is running but wxhook reports it is not logged in.",
                retryable=False,
            )

        self.self_wxid = self._get_self_wxid()
        self._messages: Queue[WechatMessage] = Queue()
        self._run_error: BaseException | None = None
        self._thread: Thread | None = None
        self.bot.handle(self.events.ALL_MESSAGE)(self._on_event)

    def iter_messages(self) -> AsyncIterator[WechatMessage]:
        return self._iter_messages()

    async def _iter_messages(self) -> AsyncIterator[WechatMessage]:
        self._ensure_running()
        while True:
            if self._run_error is not None:
                raise PipelineError(
                    ErrorCode.UNKNOWN,
                    f"wxhook listener stopped: {self._run_error}",
                )
            try:
                yield await asyncio.to_thread(self._messages.get, True, 1)
            except Empty:
                await asyncio.sleep(0.2)

    async def send_text(self, receiver: str, text: str) -> None:
        response = await asyncio.to_thread(self.bot.send_text, receiver, text)
        if not self._response_ok(response):
            raise PipelineError(ErrorCode.UNKNOWN, f"wxhook send_text failed: {response}")

    async def send_image(self, receiver: str, image_path: str | Path) -> None:
        response = await asyncio.to_thread(self.bot.send_image, receiver, str(image_path))
        if not self._response_ok(response):
            raise PipelineError(ErrorCode.UNKNOWN, f"wxhook send_image failed: {response}")

    def _ensure_running(self) -> None:
        if self._thread is not None:
            return
        self._thread = Thread(target=self._run_bot, name="wxhook-listener", daemon=True)
        self._thread.start()

    def _run_bot(self) -> None:
        try:
            self.bot.run()
        except BaseException as exc:  # pragma: no cover - hard to exercise without wxhook runtime
            self._run_error = exc

    def _prepare_wxhook_tools(self, utils: Any) -> None:
        if not self.settings.wxhook_tools_dir:
            return
        target_tools = Path(self.settings.wxhook_tools_dir)
        target_tools.mkdir(parents=True, exist_ok=True)
        source_tools = Path(utils.TOOLS)
        for source in source_tools.iterdir():
            if source.is_file():
                self._copy_wxhook_tool(source, target_tools / source.name)
        utils.TOOLS = target_tools
        utils.BASE_DIR = target_tools.parent
        utils.DLL = target_tools / "wxhook.dll"
        utils.START_WECHAT = target_tools / "start-wechat.exe"
        utils.FAKER = target_tools / "faker.exe"

    @staticmethod
    def _copy_wxhook_tool(source: Path, target: Path) -> None:
        if target.exists() and source.stat().st_size == target.stat().st_size:
            return
        try:
            shutil.copy2(source, target)
        except PermissionError:
            if target.exists() and source.stat().st_size == target.stat().st_size:
                return
            raise

    def _on_event(self, bot: Any, event: Any) -> None:
        del bot
        self._messages.put(self._normalize(event))

    def _get_self_wxid(self) -> str:
        try:
            info = self.bot.get_self_info()
        except Exception:
            return ""
        return str(getattr(info, "wxid", "") or "")

    def _check_login(self) -> bool:
        last_error: Exception | None = None
        for _ in range(5):
            try:
                return self._response_ok(self.bot.check_login())
            except Exception as exc:
                last_error = exc
                time.sleep(1)
        raise PipelineError(
            ErrorCode.CONFIG_INVALID,
            (
                "wxhook check_login failed. Check whether the installed WeChat version "
                f"is compatible with wxhook and fully logged in: {last_error}"
            ),
            retryable=False,
        )

    def _normalize(self, event: Any) -> WechatMessage:
        from_user = str(getattr(event, "fromUser", "") or "")
        to_user = str(getattr(event, "toUser", "") or "")
        group_id = _choose_chat_id(from_user, to_user)
        content = _stringify_content(
            getattr(event, "displayFullContent", None) or getattr(event, "content", "")
        )
        sender_id = from_user or to_user
        if group_id.endswith("@chatroom") and from_user == group_id:
            sender_id, content = _split_group_content(content, fallback_sender=from_user)
        msg_type = getattr(event, "type", "")
        return WechatMessage(
            group_id=group_id,
            group_name=group_id,
            sender_id=sender_id,
            sender_name=None,
            content=content,
            type=_wxhook_type_name(msg_type, self.events),
            timestamp=int(getattr(event, "createTime", 0) or 0),
            is_self=bool(self.self_wxid and sender_id == self.self_wxid),
        )

    @staticmethod
    def _response_ok(response: Any) -> bool:
        code = getattr(response, "code", None)
        numeric_code: int | None
        if isinstance(code, int):
            numeric_code = code
        elif isinstance(code, str):
            try:
                numeric_code = int(code)
            except ValueError:
                numeric_code = None
        else:
            numeric_code = None
        if numeric_code not in {0, 1, 2, 200}:
            return False
        data = getattr(response, "data", None)
        if isinstance(data, bool):
            return data
        if isinstance(data, dict):
            for key in ("status", "success", "isLogin", "login"):
                if key in data:
                    return bool(data[key])
        return True


class WcferryWechatAdapter:
    def __init__(self, settings: WechatSettings):
        self.settings = settings
        if platform.system() != "Windows":
            raise PipelineError(
                ErrorCode.CONFIG_INVALID,
                "wcferry provider only runs on Windows.",
                retryable=False,
            )
        if not _is_process_running("WeChat.exe"):
            raise PipelineError(
                ErrorCode.CONFIG_INVALID,
                "WeChat.exe is not running. Start and log in to Windows WeChat first.",
                retryable=False,
            )
        try:
            Wcf = import_module("wcferry").Wcf
        except Exception as exc:  # pragma: no cover - only exercised on Windows with wcferry
            raise PipelineError(
                ErrorCode.CONFIG_INVALID,
                "wcferry is not installed. Install with: pip install -e .[windows]",
                retryable=False,
            ) from exc
        self.wcf = self._create_wcf(Wcf)
        if settings.require_login and not self.wcf.is_login():
            raise PipelineError(
                ErrorCode.CONFIG_INVALID,
                "Windows WeChat is running but not logged in.",
                retryable=False,
            )

    def iter_messages(self) -> AsyncIterator[WechatMessage]:
        return self._iter_messages()

    async def _iter_messages(self) -> AsyncIterator[WechatMessage]:
        if not self.wcf.is_receiving_msg():
            if not self.wcf.enable_receiving_msg():
                raise PipelineError(
                    ErrorCode.UNKNOWN,
                    "wcferry failed to enable message receiving.",
                )
        while True:
            try:
                raw = await asyncio.to_thread(self.wcf.get_msg)
            except Empty:
                await asyncio.sleep(0.2)
                continue
            yield self._normalize(raw)

    async def send_text(self, receiver: str, text: str) -> None:
        code = await asyncio.to_thread(self.wcf.send_text, text, receiver)
        if code != 0:
            raise PipelineError(ErrorCode.UNKNOWN, f"wcferry send_text failed: {code}")

    async def send_image(self, receiver: str, image_path: str | Path) -> None:
        code = await asyncio.to_thread(self.wcf.send_image, str(image_path), receiver)
        if code != 0:
            raise PipelineError(ErrorCode.UNKNOWN, f"wcferry send_image failed: {code}")

    def _create_wcf(self, wcf_cls: Any) -> Any:
        original_exit = os._exit

        def raise_instead_of_exit(status: int, /) -> NoReturn:
            raise _WcferryFatalExit(status)

        try:
            os._exit = raise_instead_of_exit  # type: ignore[assignment]
            return wcf_cls(debug=self.settings.debug, port=self.settings.rpc_port, block=False)
        except _WcferryFatalExit as exc:
            raise PipelineError(
                ErrorCode.CONFIG_INVALID,
                (
                    "wcferry SDK initialization failed. Check Windows WeChat version, "
                    "login state, and process privilege level."
                ),
                retryable=False,
            ) from exc
        finally:
            os._exit = original_exit

    @staticmethod
    def _normalize(raw: Any) -> WechatMessage:
        roomid = str(getattr(raw, "roomid", "") or "")
        sender = str(getattr(raw, "sender", "") or "")
        group_id = roomid or sender
        msg_type = getattr(raw, "type", getattr(raw, "msg_type", "text"))
        content = str(getattr(raw, "content", "") or "")
        timestamp = int(getattr(raw, "ts", getattr(raw, "timestamp", 0)) or 0)
        if msg_type in {1, "1"}:
            type_name = "text"
        else:
            type_name = str(msg_type)
        raw_sender_name = getattr(raw, "sender_name", "") or sender
        return WechatMessage(
            group_id=group_id,
            group_name=group_id,
            sender_id=sender,
            sender_name=str(raw_sender_name) if raw_sender_name else None,
            content=content,
            type=type_name,
            timestamp=timestamp,
            is_self=bool(getattr(raw, "is_self", False)),
        )


class WxautoWechatAdapter:
    def __init__(self, settings: WechatSettings):
        self.settings = settings
        try:
            from wxauto import WeChat  # type: ignore[import-not-found]
        except Exception as exc:  # pragma: no cover - only exercised on Windows with wxauto
            raise PipelineError(
                ErrorCode.CONFIG_INVALID,
                "wxauto is not installed. Install with: pip install -e .[windows]",
                retryable=False,
            ) from exc
        self.wx = WeChat()

    def iter_messages(self) -> AsyncIterator[WechatMessage]:
        raise PipelineError(
            ErrorCode.CONFIG_INVALID,
            "wxauto fallback is send-only in this adapter; use provider=wxhook for listening.",
            retryable=False,
        )

    async def send_text(self, receiver: str, text: str) -> None:
        await asyncio.to_thread(self.wx.SendMsg, text, who=receiver)

    async def send_image(self, receiver: str, image_path: str | Path) -> None:
        await asyncio.to_thread(self.wx.SendFiles, str(image_path), who=receiver)


class FakeWindowsWechatAdapter:
    def __init__(self, messages: list[WechatMessage] | None = None):
        self.messages = messages or []
        self.sent_texts: list[tuple[str, str]] = []
        self.sent_images: list[tuple[str, str]] = []

    async def _iter_messages(self) -> AsyncIterator[WechatMessage]:
        for message in self.messages:
            yield message

    def iter_messages(self) -> AsyncIterator[WechatMessage]:
        return self._iter_messages()

    async def send_text(self, receiver: str, text: str) -> None:
        self.sent_texts.append((receiver, text))

    async def send_image(self, receiver: str, image_path: str | Path) -> None:
        self.sent_images.append((receiver, str(image_path)))


def build_wechat_adapter(settings: WechatSettings) -> WindowsWechatAdapter:
    provider = settings.provider.lower()
    if provider in {"fake", "mock"}:
        return FakeWindowsWechatAdapter()
    if provider in {"wxhook", "miloira_wxhook"}:
        return WxhookWechatAdapter(settings)
    if provider == "wxauto":
        return WxautoWechatAdapter(settings)
    if provider == "wcferry":
        return WcferryWechatAdapter(settings)
    raise PipelineError(ErrorCode.CONFIG_INVALID, f"Unknown WeChat provider: {settings.provider}")


class _WcferryFatalExit(RuntimeError):
    def __init__(self, code: int):
        super().__init__(f"wcferry called os._exit({code})")
        self.code = code


def _is_process_running(image_name: str) -> bool:
    try:
        result = subprocess.run(
            ["tasklist", "/FI", f"IMAGENAME eq {image_name}", "/NH"],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except Exception:
        return True
    return image_name.lower() in result.stdout.lower()


def _choose_chat_id(from_user: str, to_user: str) -> str:
    if from_user.endswith("@chatroom"):
        return from_user
    if to_user.endswith("@chatroom"):
        return to_user
    return from_user or to_user


def _split_group_content(content: str, *, fallback_sender: str) -> tuple[str, str]:
    for separator in (":\r\n", ":\n", ":"):
        if separator in content:
            sender, text = content.split(separator, 1)
            sender = sender.strip()
            if sender:
                return sender, text.lstrip()
    return fallback_sender, content


def _stringify_content(content: Any) -> str:
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    return str(content)


def _wxhook_type_name(msg_type: Any, events: Any) -> str:
    if msg_type == getattr(events, "TEXT_MESSAGE", 1):
        return "text"
    if msg_type == getattr(events, "IMAGE_MESSAGE", 3):
        return "image"
    if msg_type == getattr(events, "VIDEO_MESSAGE", 43):
        return "video"
    if msg_type == getattr(events, "VOICE_MESSAGE", 34):
        return "voice"
    return str(msg_type)
