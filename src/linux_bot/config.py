from __future__ import annotations

from enum import StrEnum
from pathlib import Path

from pydantic import BaseModel, Field, ValidationInfo, field_validator

from pipeline_core.config import load_settings, validate_secret


class MatchMode(StrEnum):
    PREFIX = "prefix"
    CONTAINS = "contains"
    REGEX = "regex"


class TimeRangeMode(StrEnum):
    BETWEEN_TRIGGERS = "between_triggers"
    FIXED_MINUTES = "fixed_minutes"
    FIXED_HOURS = "fixed_hours"
    TODAY = "today"


class ListenSettings(BaseModel):
    triggers: list[str] = Field(default_factory=lambda: ["@", "#"])
    match_mode: MatchMode = MatchMode.PREFIX
    whitelist_groups: list[str] = Field(default_factory=list)
    blacklist_users: list[str] = Field(default_factory=list)


class MessageSettings(BaseModel):
    content_types: list[str] = Field(default_factory=lambda: ["text"])
    ignore_self: bool = True


class TimeRangeSettings(BaseModel):
    mode: TimeRangeMode = TimeRangeMode.BETWEEN_TRIGGERS
    fallback_minutes: int = 30
    fixed_minutes: int = 30
    fixed_hours: int = 2


class BotSettings(BaseModel):
    cli_path: str = "wx-bot-cli"
    listen: ListenSettings = Field(default_factory=ListenSettings)
    message: MessageSettings = Field(default_factory=MessageSettings)
    time_range: TimeRangeSettings = Field(default_factory=TimeRangeSettings)


class FileTransferSettings(BaseModel):
    download_dir: str = "./runtime/linux-images"
    timeout_seconds: int = 60


class WindowsBridgeSettings(BaseModel):
    host: str = "127.0.0.1"
    port: int = 8765
    protocol: str = "websocket"
    path: str = "/ws"
    token: str
    reconnect_seconds: int = 5
    file_transfer: FileTransferSettings = Field(default_factory=FileTransferSettings)

    @field_validator("token")
    @classmethod
    def _reject_insecure_defaults(cls, value: str, info: ValidationInfo) -> str:
        return validate_secret(value, field=info.field_name or "windows_bridge.token")

    @property
    def websocket_url(self) -> str:
        scheme = "wss" if self.protocol == "wss" else "ws"
        return f"{scheme}://{self.host}:{self.port}{self.path}"


class StorageSettings(BaseModel):
    sqlite_path: str = "./runtime/linux-bot.sqlite3"


class RuntimeSettings(BaseModel):
    log_level: str = "INFO"
    metrics_enabled: bool = True


class LinuxBotConfig(BaseModel):
    bot: BotSettings = Field(default_factory=BotSettings)
    windows_bridge: WindowsBridgeSettings
    storage: StorageSettings = Field(default_factory=StorageSettings)
    runtime: RuntimeSettings = Field(default_factory=RuntimeSettings)


def load_bot_config(path: str | Path) -> LinuxBotConfig:
    return load_settings(path, LinuxBotConfig, env_prefix="LINUX_BOT")

