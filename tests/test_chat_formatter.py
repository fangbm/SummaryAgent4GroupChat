from windows_worker.chat_formatter import ChatFormatter


def test_chat_formatter_generates_stats() -> None:
    merged, chat_text, stats = ChatFormatter.format_and_stats(
        [
            {"type": "text", "timestamp": 20, "sender_name": "李四", "content": "结论"},
            {"type": "text", "timestamp": 10, "sender_name": "张三", "content": "讨论"},
            {"type": "image", "timestamp": 11, "sender_name": "张三", "content": "ignored"},
        ]
    )
    assert "[00:00] 张三: 讨论" in chat_text
    assert "用户发言统计" in merged
    assert stats["total_messages"] == 2
    assert stats["user_stats"]["张三"]["count"] == 1

