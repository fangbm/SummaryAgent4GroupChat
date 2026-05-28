from datetime import UTC, datetime
from pathlib import Path

from windows_worker.config import WxCliSettings
from windows_worker.wx_cli_adapter import CliWxClient


def test_wx_cli_export_command_matches_upstream_date_format(tmp_path) -> None:
    client = CliWxClient(WxCliSettings(executable="wx", max_messages=123))
    cmd = client.build_export_command(
        "AI群",
        datetime(2026, 5, 23, 13, 30, tzinfo=UTC),
        datetime(2026, 5, 23, 13, 45, tzinfo=UTC),
        Path(tmp_path / "out.json"),
    )
    assert cmd[:3] == ["wx", "export", "AI群"]
    assert "2026-05-23 13:30:00" in cmd
    assert "2026-05-23T13:30:00" not in cmd
    assert cmd[-2:] == ["-n", "123"]


def test_wx_cli_normalizes_upstream_message_shape() -> None:
    message = CliWxClient.normalize_message(
        {
            "timestamp": 123,
            "sender": "张三",
            "sender_username": "wxid_abc",
            "content": "hello",
            "type": "text",
        }
    )
    assert message["sender_name"] == "张三"
    assert message["sender_id"] == "wxid_abc"
