import pytest

from pipeline_core.errors import PrivacyBlockedError
from pipeline_core.privacy import PrivacyGuard


def test_privacy_redacts_and_limits_messages() -> None:
    guard = PrivacyGuard(max_messages=1, max_chars=12)
    messages = [
        {"sender_id": "wxid_abc123", "sender_name": "张三", "content": "电话 13812345678"},
        {"sender_id": "wxid_def456", "sender_name": "李四", "content": "mail a@example.com"},
    ]
    prepared = guard.prepare_messages(messages)
    assert len(prepared) == 1
    assert prepared[0]["sender_id"] == "wxid_***"
    assert "邮箱***" in prepared[0]["content"]
    assert guard.enforce_text_budget("123456789012345").endswith("[内容已按隐私预算截断]")


def test_privacy_blocks_sensitive_cloud_group() -> None:
    guard = PrivacyGuard(sensitive_groups=["g"], cloud_allowed=True)
    with pytest.raises(PrivacyBlockedError):
        guard.ensure_cloud_allowed("g", llm_is_local=False)
    guard.ensure_cloud_allowed("g", llm_is_local=True)

