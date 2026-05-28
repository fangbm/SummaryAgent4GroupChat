from __future__ import annotations

from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest

TASKS_TOTAL = Counter("wechat_pipeline_tasks_total", "Tasks by status", ["status"])
TASK_ERRORS_TOTAL = Counter("wechat_pipeline_task_errors_total", "Task errors", ["code"])
TASK_DURATION_SECONDS = Histogram("wechat_pipeline_task_duration_seconds", "Task duration")


def metrics_response() -> tuple[bytes, str]:
    return generate_latest(), CONTENT_TYPE_LATEST
