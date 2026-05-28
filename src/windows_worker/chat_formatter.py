from __future__ import annotations

from collections import Counter
from datetime import UTC, datetime
from typing import Any


def _timestamp(value: object) -> float:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
        except ValueError:
            return 0.0
    return 0.0


class ChatFormatter:
    @staticmethod
    def format_and_stats(messages: list[dict[str, Any]]) -> tuple[str, str, dict[str, Any]]:
        valid = []
        for msg in messages:
            if msg.get("type", "text") != "text":
                continue
            content = str(msg.get("content") or "").strip()
            if not content:
                continue
            item = dict(msg)
            item["_ts"] = _timestamp(msg.get("timestamp"))
            item["content"] = content
            valid.append(item)
        valid.sort(key=lambda item: item["_ts"])
        if not valid:
            return (
                "无有效聊天记录",
                "",
                {"total_messages": 0, "duration_hours": 0, "user_stats": {}},
            )

        lines = []
        for msg in valid:
            dt = datetime.fromtimestamp(float(msg["_ts"]), tz=UTC).strftime("%H:%M")
            sender = msg.get("sender_name") or msg.get("sender_id") or "未知"
            lines.append(f"[{dt}] {sender}: {msg['content']}")
        chat_text = "\n".join(lines)

        total = len(valid)
        first_ts = float(valid[0]["_ts"])
        last_ts = float(valid[-1]["_ts"])
        duration_hours = max((last_ts - first_ts) / 3600, 0.001)
        counter = Counter(msg.get("sender_name") or msg.get("sender_id") or "未知" for msg in valid)

        stats_lines = [f"===== 用户发言统计 (共 {total} 条, 时长 {duration_hours:.2f} 小时) ====="]
        for user, count in counter.most_common():
            freq = count / duration_hours
            pct = count / total * 100
            stats_lines.append(f"{user}: {count} 条 ({pct:.1f}%), 频率 {freq:.1f} 条/小时")
        stats_text = "\n".join(stats_lines)

        stats_dict = {
            "total_messages": total,
            "duration_hours": round(duration_hours, 2),
            "user_stats": {
                user: {
                    "count": count,
                    "percentage": round(count / total * 100, 1),
                    "frequency_per_hour": round(count / duration_hours, 1),
                }
                for user, count in counter.items()
            },
        }
        return f"[CHAT_RECORDS]\n{chat_text}\n\n[USER_STATS]\n{stats_text}", chat_text, stats_dict
