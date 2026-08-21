from __future__ import annotations

from pathlib import Path

from pydantic import BaseModel, Field, ValidationInfo, field_validator

from pipeline_core.config import load_settings, validate_secret


class ServerSettings(BaseModel):
    host: str = "0.0.0.0"
    port: int = 8765
    websocket_path: str = "/ws"
    metrics_enabled: bool = True


class WechatListenSettings(BaseModel):
    triggers: list[str] = Field(default_factory=lambda: ["@", "#", "/总结"])
    match_mode: str = "prefix"
    whitelist_groups: list[str] = Field(default_factory=list)
    blacklist_users: list[str] = Field(default_factory=list)
    content_types: list[str] = Field(default_factory=lambda: ["text"])
    ignore_self: bool = True


class WechatTimeRangeSettings(BaseModel):
    mode: str = "between_triggers"
    fallback_minutes: int = 30
    fixed_minutes: int = 30
    fixed_hours: int = 2


class WechatSettings(BaseModel):
    provider: str = "wxhook"
    debug: bool = False
    rpc_port: int = 10086
    require_login: bool = True
    faked_version: str | None = None
    wxhook_tools_dir: str = "C:\\Users\\Public\\wx-summary-agent\\tools"
    listen: WechatListenSettings = Field(default_factory=WechatListenSettings)
    time_range: WechatTimeRangeSettings = Field(default_factory=WechatTimeRangeSettings)
    receiver_map: dict[str, str] = Field(default_factory=dict)


class SecuritySettings(BaseModel):
    ipc_token: str
    download_secret: str
    download_url_ttl_seconds: int = 900

    @field_validator("ipc_token", "download_secret")
    @classmethod
    def _reject_insecure_defaults(cls, value: str, info: ValidationInfo) -> str:
        return validate_secret(value, field=info.field_name or "security secret")


class StorageSettings(BaseModel):
    sqlite_path: str = "./runtime/windows-worker.sqlite3"


class WxCliSettings(BaseModel):
    executable: str = "wx"
    export_format: str = "json"
    max_messages: int = 5000
    temp_dir: str = "./runtime/wx-exports"
    group_name_map: dict[str, str] = Field(default_factory=dict)


class PrivacySettings(BaseModel):
    mode: str = "protected"
    redact_enabled: bool = True
    max_messages_to_llm: int = 800
    max_chars_to_llm: int = 20_000
    cloud_allowed: bool = True
    sensitive_groups: list[str] = Field(default_factory=list)


class ProxySettings(BaseModel):
    enabled: bool = False
    http: str | None = None
    https: str | None = None

    def as_aiohttp_proxy(self) -> str | None:
        if not self.enabled:
            return None
        return self.https or self.http


class LLMSettings(BaseModel):
    provider: str = "openai_compatible"
    api_key: str = ""
    base_url: str = "https://api.openai.com/v1"
    model: str = "gpt-4o-mini"
    timeout_seconds: int = 120
    max_output_tokens: int = 2000
    temperature: float = 0.3
    proxy: ProxySettings = Field(default_factory=ProxySettings)
    system_prompt: str = "你是一位专业的群聊内容整理助手。"


class ImageGenSettings(BaseModel):
    enabled: bool = True
    provider: str = "openai"
    api_key: str = ""
    base_url: str = "https://api.openai.com/v1"
    model: str = "gpt-image-1.5"
    size: str = "1024x1536"
    quality: str = "high"
    timeout_seconds: int = 300
    proxy: ProxySettings = Field(default_factory=ProxySettings)
    prompt_template: str = "请根据以下群聊整理内容生成信息图：\n{summary}"


class FileTransferSettings(BaseModel):
    output_dir: str = "./runtime/windows-output"
    public_base_url: str = "http://127.0.0.1:8765/images"
    cleanup_after_days: int = 7


class WorkerConfig(BaseModel):
    server: ServerSettings = Field(default_factory=ServerSettings)
    wechat: WechatSettings = Field(default_factory=WechatSettings)
    security: SecuritySettings
    storage: StorageSettings = Field(default_factory=StorageSettings)
    wx_cli: WxCliSettings = Field(default_factory=WxCliSettings)
    privacy: PrivacySettings = Field(default_factory=PrivacySettings)
    llm: LLMSettings = Field(default_factory=LLMSettings)
    image_gen: ImageGenSettings = Field(default_factory=ImageGenSettings)
    file_transfer: FileTransferSettings = Field(default_factory=FileTransferSettings)


def load_worker_config(path: str | Path) -> WorkerConfig:
    return load_settings(path, WorkerConfig, env_prefix="WINDOWS_WORKER")
