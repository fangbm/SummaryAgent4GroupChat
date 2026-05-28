from __future__ import annotations

import argparse
import json
import logging
import queue
import sys
import threading
import time
from pathlib import Path
from typing import Any

PROTOCOL_STDOUT = sys.stdout
PROTOCOL_STDOUT_BUFFER = sys.stdout.buffer
EMIT_LOCK = threading.Lock()
DIAG_LOG_PATH = Path(__file__).resolve().parent.parent / "rust-agent" / "runtime" / "wx4py-sidecar.log"

# wx4py configures loggers with StreamHandler(sys.stdout). Keep stdout reserved
# for the JSON-lines protocol and send all library logs to stderr instead.
sys.stdout = sys.stderr
logging.raiseExceptions = False

from wx4py import CallbackHandler, WeChatClient


def emit(payload: dict[str, Any]) -> None:
    line = json.dumps(payload, ensure_ascii=False) + "\n"
    data = line.encode("utf-8", errors="replace")
    with EMIT_LOCK:
        try:
            PROTOCOL_STDOUT_BUFFER.write(data)
            PROTOCOL_STDOUT_BUFFER.flush()
        except OSError:
            # The Rust parent may have exited; avoid noisy wx4py callback crashes.
            pass


def diag_log(message: str) -> None:
    try:
        DIAG_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
        with DIAG_LOG_PATH.open("a", encoding="utf-8") as file:
            file.write(f"{timestamp} {message}\n")
    except OSError:
        pass


class Wx4pySidecar:
    def __init__(self, groups: list[str], ready_timeout_seconds: int):
        self.groups = groups
        self.ready_timeout_seconds = ready_timeout_seconds
        self.client = WeChatClient(auto_connect=False)
        self.send_lock = threading.Lock()
        self.stop_event = threading.Event()
        self.command_queue: queue.Queue[dict[str, Any] | None] = queue.Queue()

    def run(self) -> int:
        diag_log(f"starting groups={self.groups!r}")
        try:
            self.client.connect()
            handler = CallbackHandler(self.on_message, auto_reply=False)
            self.client.process_groups(
                self.groups,
                [handler],
                ignore_client_sent=False,
                block=False,
            )
        except Exception as exc:
            diag_log(f"startup failed: {type(exc).__name__}: {exc}")
            emit({"kind": "error", "message": f"wx4py startup failed: {type(exc).__name__}: {exc}"})
            return 2

        threading.Thread(target=self.read_commands, daemon=True).start()
        threading.Thread(target=self.run_commands, daemon=True).start()
        diag_log("ready")
        emit({"kind": "ready", "ok": True})

        try:
            while not self.stop_event.is_set():
                time.sleep(0.5)
        except KeyboardInterrupt:
            pass
        finally:
            try:
                self.client.disconnect()
            except Exception:
                pass
        return 0

    def on_message(self, event: Any) -> None:
        group = str(getattr(event, "group", "") or "")
        content = str(getattr(event, "content", "") or "")
        timestamp = int(float(getattr(event, "timestamp", time.time()) or time.time()))
        preview = content.replace("\r", " ").replace("\n", " ")[:40]
        diag_log(f"message group={group!r} len={len(content)} preview={preview!r}")
        print(
            f"[wx4py-sidecar] message group={group!r} len={len(content)} preview={preview!r}",
            file=sys.stderr,
            flush=True,
        )
        emit(
            {
                "kind": "event",
                "room_id": group,
                "room_name": group,
                "content": content,
                "sender_id": None,
                "sender_name": None,
                "timestamp": timestamp,
            }
        )

    def read_commands(self) -> None:
        while True:
            raw_bytes = sys.stdin.buffer.readline()
            if not raw_bytes:
                break
            raw = raw_bytes.decode("utf-8", errors="replace")
            raw = raw.strip()
            if not raw:
                continue
            try:
                self.command_queue.put(json.loads(raw))
            except Exception as exc:
                emit({"kind": "error", "message": f"invalid command json: {exc}"})
        self.command_queue.put(None)

    def run_commands(self) -> None:
        while not self.stop_event.is_set():
            command = self.command_queue.get()
            if command is None:
                self.stop_event.set()
                return

            try:
                self.handle_command(command)
            except Exception as exc:
                diag_log(f"command failed: {type(exc).__name__}: {exc}")
                emit(
                    {
                        "kind": "command_error",
                        "message": f"command failed: {type(exc).__name__}: {exc}",
                    }
                )

    def handle_command(self, command: dict[str, Any]) -> None:
        cmd = command.get("cmd")
        room = str(command.get("room") or "").strip()
        if not room:
            raise ValueError("room is required")

        with self.send_lock:
            if cmd == "send_text":
                text = str(command.get("text") or "").strip()
                if not text:
                    return
                registry = getattr(self.client, "outgoing_registry", None)
                if registry:
                    registry.record(room, text)
                ok = self.client.chat_window.send_to(room, text, target_type="group")
            elif cmd == "send_image":
                path = Path(str(command.get("path") or "")).resolve()
                if not path.exists():
                    raise FileNotFoundError(str(path))
                ok = self.client.chat_window.send_file_to(room, str(path), target_type="group")
            else:
                raise ValueError(f"unsupported command: {cmd}")

        if not ok:
            raise RuntimeError(f"wx4py returned false for {cmd} -> {room}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="wx4py JSON-lines sidecar for Rust agent")
    parser.add_argument("--group", action="append", default=[], help="WeChat group display name")
    parser.add_argument("--ready-timeout-seconds", type=int, default=60)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    groups = [group.strip() for group in args.group if group.strip()]
    if not groups:
        emit({"kind": "error", "message": "at least one --group is required"})
        return 2
    return Wx4pySidecar(groups, args.ready_timeout_seconds).run()


if __name__ == "__main__":
    raise SystemExit(main())
