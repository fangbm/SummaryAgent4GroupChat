param(
    [string]$Python = ".\.venv\Scripts\python.exe"
)

if (!(Test-Path -LiteralPath $Python)) {
    throw "Python executable not found: $Python"
}

$wechatCandidates = @(
    "$env:ProgramFiles\Tencent\WeChat\WeChat.exe",
    "${env:ProgramFiles(x86)}\Tencent\WeChat\WeChat.exe",
    "$env:LOCALAPPDATA\Tencent\WeChat\WeChat.exe"
)
$wechatExe = $wechatCandidates | Where-Object { Test-Path -LiteralPath $_ } | Select-Object -First 1
if ($wechatExe) {
    $wechatItem = Get-Item -LiteralPath $wechatExe
    Write-Host "WeChat path: $wechatExe"
    Write-Host "WeChat file version: $($wechatItem.VersionInfo.FileVersion)"
    Write-Host "WeChat product version: $($wechatItem.VersionInfo.ProductVersion)"
    if ($wechatItem.VersionInfo.FileVersion -notlike "3.9.5.*") {
        Write-Warning "wxhook 0.0.10 is known to target WeChat 3.9.5.81. Your installed WeChat version may be incompatible."
    }
} else {
    Write-Warning "WeChat.exe was not found in common install paths."
}

@'
import importlib.metadata
import socket
import time

import requests

from pipeline_core.errors import PipelineError
from windows_worker.config import WechatSettings
from windows_worker.wechat_adapter import build_wechat_adapter

print("wxhook version:", importlib.metadata.version("wxhook"))


def port_is_open(host, port):
    try:
        with socket.create_connection((host, port), timeout=1):
            return True
    except OSError:
        return False


def check_login(base_url):
    try:
        response = requests.post(f"{base_url}/api/checkLogin", timeout=3)
        print("check_login status:", response.status_code)
        print("check_login body:", response.text)
        payload = response.json()
        if payload.get("code") in (0, 1, 200):
            print("check_login: ok")
            return 0
        print("check_login: failed")
        return 3
    except Exception as exc:
        print("check_login error:", type(exc).__name__, exc)
        return 3


if port_is_open("127.0.0.1", 19001):
    print("api_port: listening 19001")
    print("adapter: skipped because wxhook API is already running")
    raise SystemExit(check_login("http://127.0.0.1:19001"))

try:
    adapter = build_wechat_adapter(WechatSettings(provider="wxhook", debug=False, require_login=False))
except PipelineError as exc:
    print("adapter: failed")
    print("error_code:", exc.code.value)
    print("message:", exc.message)
    raise SystemExit(2)

print("adapter: ok")
deadline = time.time() + 15
last_error = None
while time.time() < deadline:
    try:
        with socket.create_connection(("127.0.0.1", adapter.bot.remote_port), timeout=1):
            print("api_port: listening", adapter.bot.remote_port)
            break
    except OSError as exc:
        last_error = exc
        time.sleep(1)
else:
    print("api_port: not listening", adapter.bot.remote_port, last_error)
try:
    response = adapter.bot.check_login()
    print("check_login:", response)
except Exception as exc:
    print("check_login error:", type(exc).__name__, exc)
'@ | & $Python -
exit $LASTEXITCODE
