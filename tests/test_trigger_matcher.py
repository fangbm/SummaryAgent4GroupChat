from linux_bot.config import ListenSettings, MatchMode, MessageSettings
from linux_bot.trigger_matcher import TriggerMatcher


def test_prefix_trigger_matcher() -> None:
    matcher = TriggerMatcher(
        ListenSettings(
            triggers=["@", "/总结"],
            match_mode=MatchMode.PREFIX,
            whitelist_groups=["AI交流群"],
            blacklist_users=["wxid_bot"],
        ),
        MessageSettings(content_types=["text"], ignore_self=True),
    )
    msg = {
        "group_id": "123@chatroom",
        "group_name": "AI交流群一",
        "sender_id": "wxid_user",
        "sender_name": "张三",
        "content": "/总结 今天",
        "type": "text",
        "timestamp": 100,
    }
    match = matcher.match(msg)
    assert match is not None
    assert match.trigger_symbol == "/总结"
    assert matcher.match({**msg, "sender_id": "wxid_bot"}) is None

