import sys
import types

import pytest

from pipeline_core.errors import PipelineError
from windows_worker.config import WechatSettings
from windows_worker.wechat_adapter import WcferryWechatAdapter


class FakeWcf:
    init_kwargs: dict[str, object] = {}

    def __init__(self, **kwargs: object):
        self.__class__.init_kwargs = kwargs

    def is_login(self) -> bool:
        return True


class ExitingWcf:
    def __init__(self, **kwargs: object):
        import os

        os._exit(1)


def test_wcferry_adapter_uses_nonblocking_start(monkeypatch) -> None:
    fake_module = types.SimpleNamespace(Wcf=FakeWcf)
    monkeypatch.setitem(sys.modules, "wcferry", fake_module)
    monkeypatch.setattr("windows_worker.wechat_adapter.platform.system", lambda: "Windows")
    monkeypatch.setattr("windows_worker.wechat_adapter._is_process_running", lambda _: True)

    adapter = WcferryWechatAdapter(WechatSettings(provider="wcferry", rpc_port=10087))

    assert isinstance(adapter.wcf, FakeWcf)
    assert FakeWcf.init_kwargs == {"debug": False, "port": 10087, "block": False}


def test_wcferry_adapter_converts_sdk_exit_to_pipeline_error(monkeypatch) -> None:
    fake_module = types.SimpleNamespace(Wcf=ExitingWcf)
    monkeypatch.setitem(sys.modules, "wcferry", fake_module)
    monkeypatch.setattr("windows_worker.wechat_adapter.platform.system", lambda: "Windows")
    monkeypatch.setattr("windows_worker.wechat_adapter._is_process_running", lambda _: True)

    with pytest.raises(PipelineError) as exc:
        WcferryWechatAdapter(WechatSettings(provider="wcferry"))

    assert "wcferry SDK initialization failed" in str(exc.value)


def test_wcferry_adapter_requires_wechat_process(monkeypatch) -> None:
    monkeypatch.setattr("windows_worker.wechat_adapter.platform.system", lambda: "Windows")
    monkeypatch.setattr("windows_worker.wechat_adapter._is_process_running", lambda _: False)

    with pytest.raises(PipelineError) as exc:
        WcferryWechatAdapter(WechatSettings(provider="wcferry"))

    assert "WeChat.exe is not running" in str(exc.value)
