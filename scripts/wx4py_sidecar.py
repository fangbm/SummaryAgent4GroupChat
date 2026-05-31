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
APP_ROOT = Path(__file__).resolve().parent.parent
if (APP_ROOT / "rust-agent" / "config").exists():
    DIAG_LOG_PATH = APP_ROOT / "rust-agent" / "runtime" / "wx4py-sidecar.log"
else:
    DIAG_LOG_PATH = APP_ROOT / "runtime" / "wx4py-sidecar.log"

# wx4py configures loggers with StreamHandler(sys.stdout). Keep stdout reserved
# for the JSON-lines protocol and send all library logs to stderr instead.
sys.stdout = sys.stderr
logging.raiseExceptions = False

from wx4py import CallbackHandler, WeChatClient
from wx4py.features.messaging import WeChatGroupProcessor


GROUP_RETRY_SECONDS = 30.0


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
        self.active_send_room: str | None = None
        self.processors: dict[str, WeChatGroupProcessor] = {}
        self.pending_groups: set[str] = set()

    def run(self) -> int:
        diag_log(f"starting groups={self.groups!r}")
        try:
            self.client.connect()
            self.start_group_processors(self.groups)
        except Exception as exc:
            diag_log(f"startup failed: {type(exc).__name__}: {exc}")
            emit({"kind": "error", "message": f"wx4py startup failed: {type(exc).__name__}: {exc}"})
            return 2

        if not self.processors:
            message = "no WeChat groups initialized at startup; will keep retrying in background"
            diag_log(message)

        threading.Thread(target=self.read_commands, daemon=True).start()
        threading.Thread(target=self.run_commands, daemon=True).start()
        threading.Thread(target=self.retry_pending_groups, daemon=True).start()
        diag_log("ready")
        emit({"kind": "ready", "ok": True})

        try:
            while not self.stop_event.is_set():
                time.sleep(0.5)
        except KeyboardInterrupt:
            pass
        finally:
            self.stop_processors()
            try:
                self.client.disconnect()
            except Exception:
                pass
        return 0

    def start_group_processors(self, groups: list[str]) -> None:
        for group in groups:
            if group in self.processors:
                continue
            try:
                self.start_group_processor(group)
            except Exception as exc:
                self.pending_groups.add(group)
                diag_log(f"group listener start failed group={group!r}: {type(exc).__name__}: {exc}")

    def start_group_processor(self, group: str) -> None:
        handler = CallbackHandler(self.on_message, auto_reply=False)
        processor = WeChatGroupProcessor(
            self.client,
            [group],
            [handler],
            ignore_client_sent=False,
        )
        try:
            with self.send_lock:
                processor.start(block=False)
        except Exception:
            try:
                processor.stop()
            except Exception:
                pass
            raise

        self.processors[group] = processor
        self.pending_groups.discard(group)
        diag_log(f"group listener ready group={group!r}")

    def retry_pending_groups(self) -> None:
        while not self.stop_event.wait(GROUP_RETRY_SECONDS):
            for group in list(self.pending_groups):
                if group in self.processors:
                    self.pending_groups.discard(group)
                    continue
                try:
                    diag_log(f"retrying group listener group={group!r}")
                    self.start_group_processor(group)
                except Exception as exc:
                    diag_log(
                        f"group listener retry failed group={group!r}: {type(exc).__name__}: {exc}"
                    )

    def stop_processors(self) -> None:
        for processor in list(self.processors.values()):
            try:
                processor.stop()
            except Exception:
                pass
        self.processors.clear()

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
                ok = self.send_text_guarded(room, text)
            elif cmd == "send_image":
                path = Path(str(command.get("path") or "")).resolve()
                if not path.exists():
                    raise FileNotFoundError(str(path))
                ok = self.send_image_guarded(room, path)
            else:
                raise ValueError(f"unsupported command: {cmd}")

        if not ok:
            raise RuntimeError(f"wx4py returned false for {cmd} -> {room}")

    def open_chat_guarded(self, room: str) -> bool:
        # wx4py can report "chat opened" before the Qt view has actually switched.
        # When changing rooms, prime the target twice and settle before pasting.
        rounds = 2 if self.active_send_room != room else 1
        for index in range(rounds):
            diag_log(
                f"open_guard target={room!r} previous={self.active_send_room!r} round={index + 1}/{rounds}"
            )
            if not self.open_chat_once_guarded(room):
                self.active_send_room = None
                return False
            time.sleep(1.2)
        self.active_send_room = room
        return True

    def open_chat_once_guarded(self, room: str) -> bool:
        try:
            ok = self.client.chat_window.open_chat(
                room,
                target_type="group",
                raise_on_target_not_found=True,
            )
            if ok:
                return True
            diag_log(f"open_guard primary returned false target={room!r}")
        except Exception as exc:
            diag_log(
                f"open_guard primary failed target={room!r}: {type(exc).__name__}: {exc}"
            )

        return self.open_chat_from_search_fallback(room)

    def open_chat_from_search_fallback(self, room: str) -> bool:
        try:
            chat_window = self.client.chat_window
            results = chat_window.search(room)
            self.log_search_results(room, results)

            for item in self.iter_matching_search_results(room, results):
                group = str(getattr(item, "group", "") or "")
                name = str(getattr(item, "name", "") or "")
                diag_log(
                    f"open_fallback clicking target={room!r} group={group!r} name={name!r}"
                )
                if not self.click_search_result(item):
                    continue
                time.sleep(1.0)
                get_chat_input = getattr(chat_window, "_get_chat_input", None)
                if callable(get_chat_input):
                    try:
                        if not get_chat_input():
                            diag_log(
                                f"open_fallback clicked but no chat input target={room!r} group={group!r} name={name!r}"
                            )
                            continue
                    except Exception as exc:
                        diag_log(
                            f"open_fallback input check failed target={room!r}: {type(exc).__name__}: {exc}"
                        )
                        continue
                diag_log(
                    f"open_fallback succeeded target={room!r} group={group!r} name={name!r}"
                )
                return True
        except Exception as exc:
            diag_log(
                f"open_fallback failed target={room!r}: {type(exc).__name__}: {exc}"
            )

        try:
            clear_search = getattr(self.client.chat_window, "_clear_search", None)
            if callable(clear_search):
                clear_search()
        except Exception:
            pass
        return False

    def iter_matching_search_results(self, room: str, results: dict[str, Any]):
        target = self.normalize_search_text(room)
        preferred_groups = ["最常使用", "群聊", "聊天记录", "联系人", "未知"]
        excluded_groups = {"功能", "搜索网络结果"}
        ordered_groups = preferred_groups + [
            group for group in results.keys() if group not in preferred_groups
        ]

        seen_ids: set[int] = set()
        for exact_only in (True, False):
            for group in ordered_groups:
                if group in excluded_groups:
                    continue
                for item in results.get(group, []):
                    item_id = id(item)
                    if item_id in seen_ids:
                        continue
                    name = self.normalize_search_text(getattr(item, "name", ""))
                    if not name:
                        continue
                    matched = name == target if exact_only else target in name
                    if matched:
                        seen_ids.add(item_id)
                        yield item

    def normalize_search_text(self, value: Any) -> str:
        return " ".join(str(value or "").split()).strip()

    def log_search_results(self, room: str, results: dict[str, Any]) -> None:
        summary: dict[str, list[str]] = {}
        for group, items in results.items():
            names = []
            for item in items[:5]:
                name = self.normalize_search_text(getattr(item, "name", ""))
                if name:
                    names.append(name[:80])
            summary[group] = names
        diag_log(f"open_fallback search target={room!r} groups={summary!r}")

    def click_search_result(self, item: Any) -> bool:
        ctrl = getattr(item, "ctrl", None)
        if ctrl is None:
            return False
        for method_name, kwargs in (
            ("Click", {}),
            ("Click", {"simulateMove": False}),
            ("DoubleClick", {"simulateMove": False}),
        ):
            try:
                getattr(ctrl, method_name)(**kwargs)
                return True
            except Exception as exc:
                diag_log(
                    f"open_fallback {method_name} failed name={getattr(item, 'name', '')!r}: {type(exc).__name__}: {exc}"
                )
        return False

    def send_text_guarded(self, room: str, text: str) -> bool:
        for attempt in range(1, 3):
            try:
                diag_log(f"send_text_guarded target={room!r} attempt={attempt}")
                if not self.open_chat_guarded(room):
                    continue
                registry = getattr(self.client, "outgoing_registry", None)
                if registry:
                    registry.record(room, text)
                if self.client.chat_window.send_message(text):
                    return True
            except Exception as exc:
                diag_log(f"send_text_guarded failed target={room!r}: {type(exc).__name__}: {exc}")
            self.active_send_room = None
            time.sleep(1.0)
        return False

    def send_image_guarded(self, room: str, path: Path) -> bool:
        for attempt in range(1, 3):
            try:
                diag_log(f"send_image_guarded target={room!r} attempt={attempt}")
                if not self.open_chat_guarded(room):
                    continue
                if self.client.chat_window.send_file(str(path)):
                    return True
            except Exception as exc:
                diag_log(f"send_image_guarded failed target={room!r}: {type(exc).__name__}: {exc}")
            self.active_send_room = None
            time.sleep(1.0)
        return False


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
