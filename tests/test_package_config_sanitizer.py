from __future__ import annotations

import shutil
import subprocess
import tomllib
from pathlib import Path

import pytest


def run_sanitizer(source: Path, destination: Path) -> str:
    powershell = shutil.which("pwsh") or shutil.which("powershell")
    if powershell is None:
        pytest.skip("PowerShell is required for the Windows package sanitizer")

    script = Path(__file__).parents[1] / "scripts" / "sanitize-agent-config.ps1"
    subprocess.run(
        [
            powershell,
            "-NoProfile",
            "-File",
            str(script),
            "-Source",
            str(source),
            "-Destination",
            str(destination),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    return destination.read_text(encoding="utf-8-sig")


def assert_no_sensitive_assignments(value: object) -> None:
    markers = (
        "apikey",
        "accesstoken",
        "refreshtoken",
        "idtoken",
        "token",
        "password",
        "passwd",
        "clientsecret",
        "secret",
        "authorization",
        "credential",
        "privatekey",
    )
    if isinstance(value, dict):
        for key, nested in value.items():
            normalized = "".join(character for character in key.lower() if character.isalnum())
            if not normalized.endswith(("env", "envvar")):
                assert not any(marker in normalized for marker in markers), key
            assert_no_sensitive_assignments(nested)
    elif isinstance(value, list):
        for nested in value:
            assert_no_sensitive_assignments(nested)


def test_package_config_sanitizer_removes_secret_variants_and_keeps_env(tmp_path: Path) -> None:
    source = tmp_path / "agent.toml"
    destination = tmp_path / "sanitized.toml"
    source.write_text(
        """
[llm]
api_key = "sk-direct"
api_key_env = "LLM_API_KEY"
access_token = "access-direct"
client_secret = "client-direct"
authorization = "Bearer direct"
password = "password-direct"
token_env = "DISCORD_BOT_TOKEN"
request_body_overrides = { authorization = "nested-direct", access_token_env = "NESTED_TOKEN" }
model = "safe-model"
""".strip(),
        encoding="utf-8",
    )

    sanitized = run_sanitizer(source, destination)
    parsed = tomllib.loads(sanitized)
    assert_no_sensitive_assignments(parsed)
    assert "sk-direct" not in sanitized
    assert "access-direct" not in sanitized
    assert "client-direct" not in sanitized
    assert "Bearer direct" not in sanitized
    assert "nested-direct" not in sanitized
    assert "password-direct" not in sanitized
    assert 'api_key_env = "LLM_API_KEY"' in sanitized
    assert 'token_env = "DISCORD_BOT_TOKEN"' in sanitized
    assert 'access_token_env = "NESTED_TOKEN"' in sanitized
    assert 'model = "safe-model"' in sanitized


def test_package_config_sanitizer_keeps_inline_tables_valid_in_all_positions(
    tmp_path: Path,
) -> None:
    source = tmp_path / "agent.toml"
    destination = tmp_path / "sanitized.toml"
    source.write_text(
        """
[llm]
inline_head = { api_key = "head-secret", model = "safe-head" }
inline_middle = { model = "safe-middle", credential = "middle-secret", timeout = 30 }
inline_tail = { model = "safe-tail", private_key = "tail-secret" }
only_sensitive = { refresh_token = "only-secret" }
request_body_overrides = { model = "x", nested = { id_token = "s" }, access_token_env = "T" }
""".strip(),
        encoding="utf-8",
    )

    sanitized = run_sanitizer(source, destination)
    parsed = tomllib.loads(sanitized)
    assert_no_sensitive_assignments(parsed)
    assert parsed["llm"]["inline_head"]["model"] == "safe-head"
    assert parsed["llm"]["inline_middle"]["timeout"] == 30
    assert parsed["llm"]["inline_tail"]["model"] == "safe-tail"
    assert parsed["llm"]["only_sensitive"] == {}
    assert parsed["llm"]["request_body_overrides"]["nested"] == {}
    assert parsed["llm"]["request_body_overrides"]["access_token_env"] == "T"


def test_package_config_sanitizer_removes_quoted_keys_at_each_toml_level(
    tmp_path: Path,
) -> None:
    source = tmp_path / "agent.toml"
    destination = tmp_path / "sanitized.toml"
    source.write_text(
        """
"api-key" = "top-secret"
'token' = 'top-token'
"api-key-env" = "TOP_API_KEY"
'token_env' = 'TOP_TOKEN'

[llm]
"client-secret" = "table-secret"
'password' = 'table-password'
"access-token-env" = "TABLE_TOKEN"
request_body_overrides = { "api-key" = "inline-secret", model = "safe", \
nested = { 'token' = 'nested-secret', 'token_env' = 'NESTED_TOKEN' } }
""".strip(),
        encoding="utf-8",
    )

    sanitized = run_sanitizer(source, destination)
    parsed = tomllib.loads(sanitized)
    assert_no_sensitive_assignments(parsed)
    assert parsed["api-key-env"] == "TOP_API_KEY"
    assert parsed["token_env"] == "TOP_TOKEN"
    assert parsed["llm"]["access-token-env"] == "TABLE_TOKEN"
    assert parsed["llm"]["request_body_overrides"] == {
        "model": "safe",
        "nested": {"token_env": "NESTED_TOKEN"},
    }
    for secret in (
        "top-secret",
        "top-token",
        "table-secret",
        "table-password",
        "inline-secret",
        "nested-secret",
    ):
        assert secret not in sanitized


def test_windows_installer_does_not_force_non_elevated_gui_launch() -> None:
    script = (
        Path(__file__).parents[1] / "scripts" / "package-windows-installer.ps1"
    ).read_text(encoding="utf-8")
    run_section = script.split("[Run]", 1)[1].split("[Code]", 1)[0]

    assert "PrivilegesRequired=admin" in script
    assert "runascurrentuser" not in run_section.lower()
    assert "Flags: nowait postinstall skipifsilent" in run_section
