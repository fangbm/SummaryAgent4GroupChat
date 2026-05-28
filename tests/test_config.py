import os

from pydantic import BaseModel

from pipeline_core.config import load_settings


class Nested(BaseModel):
    value: int


class Settings(BaseModel):
    nested: Nested
    token: str


def test_config_placeholders_and_env_override(tmp_path, monkeypatch) -> None:
    path = tmp_path / "config.yaml"
    path.write_text("nested:\n  value: 1\ntoken: ${TOKEN:-fallback}\n", encoding="utf-8")
    monkeypatch.setenv("TOKEN", "from-env")
    monkeypatch.setenv("APP__NESTED__VALUE", "2")
    settings = load_settings(path, Settings, env_prefix="APP")
    assert settings.token == "from-env"
    assert settings.nested.value == 2
    os.environ.pop("APP__NESTED__VALUE", None)

