from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


class SQLiteStore:
    def __init__(self, path: str | Path):
        self.path = str(path)
        if self.path != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self.init_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        return conn

    def init_schema(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS group_state (
                    group_id TEXT PRIMARY KEY,
                    last_trigger_ts INTEGER NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS tasks (
                    request_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    group_id TEXT,
                    payload_json TEXT NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    error_code TEXT,
                    error_message TEXT,
                    summary_text TEXT,
                    image_filename TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )

    def get_last_trigger(self, group_id: str) -> int | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT last_trigger_ts FROM group_state WHERE group_id = ?",
                (group_id,),
            ).fetchone()
        return None if row is None else int(row["last_trigger_ts"])

    def set_last_trigger(self, group_id: str, timestamp: int) -> None:
        now = _now_iso()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO group_state(group_id, last_trigger_ts, updated_at)
                VALUES(?, ?, ?)
                ON CONFLICT(group_id) DO UPDATE SET
                    last_trigger_ts = excluded.last_trigger_ts,
                    updated_at = excluded.updated_at
                """,
                (group_id, timestamp, now),
            )

    def upsert_task(
        self,
        request_id: str,
        *,
        status: str,
        group_id: str | None,
        payload: dict[str, Any],
    ) -> None:
        now = _now_iso()
        payload_json = json.dumps(payload, ensure_ascii=False, sort_keys=True)
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO tasks(
                    request_id, status, group_id, payload_json, created_at, updated_at
                )
                VALUES(?, ?, ?, ?, ?, ?)
                ON CONFLICT(request_id) DO UPDATE SET
                    status = excluded.status,
                    group_id = excluded.group_id,
                    payload_json = excluded.payload_json,
                    updated_at = excluded.updated_at
                """,
                (request_id, status, group_id, payload_json, now, now),
            )

    def get_task(self, request_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM tasks WHERE request_id = ?", (request_id,)).fetchone()
        if row is None:
            return None
        data = dict(row)
        data["payload"] = json.loads(data.pop("payload_json"))
        return data

    def update_task(
        self,
        request_id: str,
        *,
        status: str | None = None,
        attempts: int | None = None,
        error_code: str | None = None,
        error_message: str | None = None,
        summary_text: str | None = None,
        image_filename: str | None = None,
    ) -> None:
        updates: dict[str, Any] = {"updated_at": _now_iso()}
        if status is not None:
            updates["status"] = status
        if attempts is not None:
            updates["attempts"] = attempts
        if error_code is not None:
            updates["error_code"] = error_code
        if error_message is not None:
            updates["error_message"] = error_message
        if summary_text is not None:
            updates["summary_text"] = summary_text
        if image_filename is not None:
            updates["image_filename"] = image_filename

        assignments = ", ".join(f"{key} = ?" for key in updates)
        values = [*updates.values(), request_id]
        with self._connect() as conn:
            conn.execute(f"UPDATE tasks SET {assignments} WHERE request_id = ?", values)

    def increment_attempts(self, request_id: str) -> int:
        with self._connect() as conn:
            conn.execute(
                "UPDATE tasks SET attempts = attempts + 1, updated_at = ? WHERE request_id = ?",
                (_now_iso(), request_id),
            )
            row = conn.execute(
                "SELECT attempts FROM tasks WHERE request_id = ?",
                (request_id,),
            ).fetchone()
        return 0 if row is None else int(row["attempts"])

    def list_tasks(self, statuses: Iterable[str]) -> list[dict[str, Any]]:
        placeholders = ", ".join("?" for _ in statuses)
        statuses_list = list(statuses)
        if not statuses_list:
            return []
        with self._connect() as conn:
            rows = conn.execute(
                f"SELECT * FROM tasks WHERE status IN ({placeholders}) ORDER BY created_at ASC",
                statuses_list,
            ).fetchall()
        tasks = []
        for row in rows:
            item = dict(row)
            item["payload"] = json.loads(item.pop("payload_json"))
            tasks.append(item)
        return tasks
