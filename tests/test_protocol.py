from datetime import UTC, datetime

from pipeline_core.protocol import MessageType, SignalMessage, TriggerDetectedPayload


def test_signal_message_roundtrip() -> None:
    payload = TriggerDetectedPayload(
        group_id="g@chatroom",
        trigger_user="wxid_a",
        trigger_content="@总结",
        trigger_symbol="@",
        trigger_time=datetime(2026, 5, 23, tzinfo=UTC),
        since=datetime(2026, 5, 23, 0, 0, tzinfo=UTC),
        until=datetime(2026, 5, 23, 1, 0, tzinfo=UTC),
        time_range_mode="between_triggers",
        request_id="req-1",
    )
    signal = SignalMessage.from_payload(MessageType.TRIGGER_DETECTED, payload)
    restored = SignalMessage.model_validate_json(signal.model_dump_json())
    assert restored.schema_version == "1.0"
    assert restored.type == MessageType.TRIGGER_DETECTED
    assert restored.payload["request_id"] == "req-1"

