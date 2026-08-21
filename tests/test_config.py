import os

import pytest
from pydantic import BaseModel, ValidationError

from pipeline_core.config import load_settings, validate_secret
from windows_worker.config import SecuritySettings


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


@pytest.mark.parametrize("insecure", ["change-me", "", "  ", "Change-Me-Download"])
def test_security_settings_reject_insecure_placeholders(insecure: str) -> None:
    with pytest.raises(ValidationError):
        SecuritySettings(ipc_token=insecure, download_secret="strong-value")


def test_security_settings_rejects_placeholder_download_secret() -> None:
    with pytest.raises(ValidationError):
        SecuritySettings(ipc_token="strong-token", download_secret="changeme")


def test_security_settings_accept_strong_values() -> None:
    settings = SecuritySettings(
        ipc_token="x8Kp2vQm5tRw9zLc",
        download_secret="y3Nf7bHd1sJg6vKa",
    )
    assert settings.ipc_token == "x8Kp2vQm5tRw9zLc"


def test_validate_secret_reports_field_name() -> None:
    with pytest.raises(Exception, match="download_secret"):
        validate_secret("change-me", field="download_secret")

