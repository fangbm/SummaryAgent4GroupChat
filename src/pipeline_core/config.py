from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any, TypeVar, cast

import yaml
from pydantic import BaseModel, ValidationError

from pipeline_core.errors import ErrorCode, PipelineError

T = TypeVar("T", bound=BaseModel)

ENV_PATTERN = re.compile(r"\$\{([A-Z0-9_]+)(?::-(.*?))?\}")


def _parse_env_scalar(value: str) -> Any:
    try:
        return yaml.safe_load(value)
    except yaml.YAMLError:
        return value


def _resolve_placeholders(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: _resolve_placeholders(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_resolve_placeholders(item) for item in value]
    if not isinstance(value, str):
        return value

    def replace(match: re.Match[str]) -> str:
        name = match.group(1)
        default = match.group(2) or ""
        return os.getenv(name, default)

    return ENV_PATTERN.sub(replace, value)


def _set_deep(data: dict[str, Any], path: list[str], value: Any) -> None:
    cursor = data
    for key in path[:-1]:
        current = cursor.get(key)
        if not isinstance(current, dict):
            current = {}
            cursor[key] = current
        cursor = current
    cursor[path[-1]] = value


def _apply_env_overrides(data: dict[str, Any], env_prefix: str | None) -> dict[str, Any]:
    if not env_prefix:
        return data
    prefix = f"{env_prefix.upper()}__"
    for key, value in os.environ.items():
        if not key.startswith(prefix):
            continue
        path = [part.lower() for part in key.removeprefix(prefix).split("__") if part]
        if path:
            _set_deep(data, path, _parse_env_scalar(value))
    return data


def load_yaml_config(path: str | Path) -> dict[str, Any]:
    config_path = Path(path)
    if not config_path.exists():
        raise PipelineError(ErrorCode.CONFIG_INVALID, f"Config file not found: {config_path}")
    with config_path.open("r", encoding="utf-8") as fh:
        raw = yaml.safe_load(fh) or {}
    if not isinstance(raw, dict):
        raise PipelineError(ErrorCode.CONFIG_INVALID, "Config root must be a mapping")
    return cast(dict[str, Any], _resolve_placeholders(raw))


def load_settings(path: str | Path, model: type[T], *, env_prefix: str | None = None) -> T:
    data = _apply_env_overrides(load_yaml_config(path), env_prefix)
    try:
        return model.model_validate(data)
    except ValidationError as exc:
        raise PipelineError(ErrorCode.CONFIG_INVALID, str(exc), retryable=False) from exc
