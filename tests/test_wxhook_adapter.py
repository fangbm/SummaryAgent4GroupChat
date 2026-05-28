import sys
import types
from dataclasses import dataclass

import pytest

from pipeline_core.errors import PipelineError
from windows_worker.config import WechatSettings
from windows_worker.wechat_adapter import WxhookWechatAdapter, build_wechat_adapter


@dataclass
class FakeResponse:
    code: int = 200
    data: object = None
    msg: str = "success"


@dataclass
class FakeSelfInfo:
    wxid: str = "wxid_self"


class FakeBot:
    init_kwargs: dict[str, object] = {}

    def __init__(self, **kwargs: object):
        self.__class__.init_kwargs = kwargs
        self.handlers: list[object] = []
        self.sent_texts: list[tuple[str, str]] = []
        self.sent_images: list[tuple[str, str]] = []

    def check_login(self) -> FakeResponse:
        return FakeResponse(data={"status": True})

    def get_self_info(self) -> FakeSelfInfo:
        return FakeSelfInfo()

    def handle(self, event: object, once: bool = False):
        del event, once

        def wrapper(func: object) -> None:
            self.handlers.append(func)

        return wrapper

    def send_text(self, wxid: str, msg: str) -> FakeResponse:
        self.sent_texts.append((wxid, msg))
        return FakeResponse()

    def send_image(self, wxid: str, image_path: str) -> FakeResponse:
        self.sent_images.append((wxid, image_path))
        return FakeResponse()

    def run(self) -> None:
        return None


class BrokenBot(FakeBot):
    def check_login(self) -> FakeResponse:
        return FakeResponse(data={"status": False})


def install_fake_wxhook(monkeypatch, bot_cls: type[FakeBot] = FakeBot) -> None:
    monkeypatch.setitem(sys.modules, "wxhook", types.SimpleNamespace(Bot=bot_cls))
    monkeypatch.setitem(
        sys.modules,
        "wxhook.events",
        types.SimpleNamespace(ALL_MESSAGE=99999, TEXT_MESSAGE=1, IMAGE_MESSAGE=3),
    )
    monkeypatch.setitem(sys.modules, "wxhook.utils", types.SimpleNamespace())
    monkeypatch.setattr("windows_worker.wechat_adapter.platform.system", lambda: "Windows")


def test_wxhook_adapter_is_default_provider(monkeypatch) -> None:
    install_fake_wxhook(monkeypatch)

    adapter = build_wechat_adapter(
        WechatSettings(faked_version="3.9.5.81", wxhook_tools_dir="")
    )

    assert isinstance(adapter, WxhookWechatAdapter)
    assert FakeBot.init_kwargs == {"faked_version": "3.9.5.81"}


def test_wxhook_adapter_requires_login(monkeypatch) -> None:
    install_fake_wxhook(monkeypatch, BrokenBot)

    with pytest.raises(PipelineError) as exc:
        WxhookWechatAdapter(WechatSettings(provider="wxhook", wxhook_tools_dir=""))

    assert "not logged in" in str(exc.value)


def test_wxhook_response_ok_accepts_native_success_code_two() -> None:
    assert WxhookWechatAdapter._response_ok(FakeResponse(code=2))


async def test_wxhook_adapter_sends_text_and_image(monkeypatch, tmp_path) -> None:
    install_fake_wxhook(monkeypatch)
    adapter = WxhookWechatAdapter(WechatSettings(provider="wxhook", wxhook_tools_dir=""))
    image_path = tmp_path / "summary.png"
    image_path.write_bytes(b"png")

    await adapter.send_text("room@chatroom", "hello")
    await adapter.send_image("room@chatroom", image_path)

    assert adapter.bot.sent_texts == [("room@chatroom", "hello")]
    assert adapter.bot.sent_images == [("room@chatroom", str(image_path))]


def test_wxhook_adapter_normalizes_group_text_message(monkeypatch) -> None:
    install_fake_wxhook(monkeypatch)
    adapter = WxhookWechatAdapter(WechatSettings(provider="wxhook", wxhook_tools_dir=""))
    event = types.SimpleNamespace(
        fromUser="room@chatroom",
        toUser="wxid_self",
        content="wxid_friend:\n/总结 今天",
        displayFullContent=None,
        type=1,
        createTime=1_769_000_000,
    )

    message = adapter._normalize(event)

    assert message.group_id == "room@chatroom"
    assert message.sender_id == "wxid_friend"
    assert message.content == "/总结 今天"
    assert message.type == "text"
    assert not message.is_self


def test_wxhook_tool_copy_reuses_existing_same_size_file(monkeypatch, tmp_path) -> None:
    source = tmp_path / "source" / "wxhook.dll"
    target = tmp_path / "target" / "wxhook.dll"
    source.parent.mkdir()
    target.parent.mkdir()
    source.write_bytes(b"same")
    target.write_bytes(b"old!")

    def fail_copy(*args: object, **kwargs: object) -> None:
        raise PermissionError("locked")

    monkeypatch.setattr("windows_worker.wechat_adapter.shutil.copy2", fail_copy)

    WxhookWechatAdapter._copy_wxhook_tool(source, target)

    assert target.read_bytes() == b"old!"
