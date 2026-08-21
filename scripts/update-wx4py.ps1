<#
.SYNOPSIS
    Upgrades pip packages (wx4py by default) inside the managed virtual
    environment only. Deliberately separate from install-python-runtime.ps1
    so dependency updates do not touch wxdb or rewrite the whole config.
#>
param(
    [string]$RootPath = "",
    [string]$ConfigPath,
    [string[]]$PackageName = @("wx4py")
)

$ErrorActionPreference = "Stop"
$ProgressPreference = "SilentlyContinue"
# Windows PowerShell 5.1 otherwise writes Chinese status text using the active
# ANSI code page when its output is captured by the Rust control service.
$Utf8OutputEncoding = New-Object System.Text.UTF8Encoding($false)
[Console]::InputEncoding = $Utf8OutputEncoding
[Console]::OutputEncoding = $Utf8OutputEncoding
$OutputEncoding = $Utf8OutputEncoding
$env:PYTHONUTF8 = "1"
$env:PYTHONIOENCODING = "utf-8"

function Write-Step {
    param([string]$Message)
    Write-Host "[pip-update] $Message"
}

if ([string]::IsNullOrWhiteSpace($RootPath)) {
    $RootPath = if ((Split-Path -Leaf $PSScriptRoot) -eq "scripts") {
        Split-Path -Parent $PSScriptRoot
    } else {
        $PSScriptRoot
    }
}
$RootPath = (Resolve-Path -LiteralPath $RootPath).Path
if ([string]::IsNullOrWhiteSpace($ConfigPath)) {
    $ConfigPath = Join-Path $RootPath "config\agent.toml"
}
if (-not [System.IO.Path]::IsPathRooted($ConfigPath)) {
    $ConfigPath = Join-Path $RootPath $ConfigPath
}
$ConfigPath = [System.IO.Path]::GetFullPath($ConfigPath)
$ConfigBasePath = if ((Split-Path -Leaf (Split-Path -Parent $ConfigPath)) -eq "config") {
    Split-Path -Parent (Split-Path -Parent $ConfigPath)
} else {
    $RootPath
}

# Prefer the interpreter configured for the agent; fall back to the managed venv.
$pythonExecutable = $null
if (Test-Path -LiteralPath $ConfigPath) {
    $text = [System.IO.File]::ReadAllText($ConfigPath)
    $match = [regex]::Match($text, '(?ms)\[wx4py\].*?^python_executable\s*=\s*"([^"]*)"')
    if ($match.Success -and -not [string]::IsNullOrWhiteSpace($match.Groups[1].Value)) {
        $configured = $match.Groups[1].Value.Replace('\\', '\')
        if (-not [System.IO.Path]::IsPathRooted($configured)) {
            $configured = Join-Path $ConfigBasePath $configured
        }
        $configured = [System.IO.Path]::GetFullPath($configured)
        if (Test-Path -LiteralPath $configured) {
            $pythonExecutable = $configured
        }
    }
}
if (-not $pythonExecutable) {
    $venvPython = Join-Path $RootPath ".venv\Scripts\python.exe"
    if (Test-Path -LiteralPath $venvPython) {
        $pythonExecutable = $venvPython
    }
}
if (-not $pythonExecutable) {
    throw "未找到可用的 Python 解释器。请先在 GUI 中运行安装微信运行环境。"
}

Write-Step "使用解释器：$pythonExecutable"
Write-Step "正在升级：$($PackageName -join ', ')"
& $pythonExecutable -m pip install --disable-pip-version-check --upgrade @PackageName
if ($LASTEXITCODE -ne 0) {
    throw "pip 升级失败，退出码 $LASTEXITCODE"
}

foreach ($package in $PackageName) {
    $show = & $pythonExecutable -m pip show $package 2>$null
    $versionLine = $show | Where-Object { $_ -like "Version:*" } | Select-Object -First 1
    if ($versionLine) {
        Write-Step "$package 当前版本：$($versionLine.Substring('Version:'.Length).Trim())"
    }
}

Write-Step "依赖更新完成。建议重启主程序以加载新版本。"
