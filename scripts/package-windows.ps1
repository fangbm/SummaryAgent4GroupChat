param(
    [string]$OutDir,
    [switch]$SkipBuild
)

$ErrorActionPreference = "Stop"

$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$RustRoot = Join-Path $RepoRoot "rust-agent"
if ([string]::IsNullOrWhiteSpace($OutDir)) {
    $OutDir = Join-Path $RepoRoot "dist"
}

if (-not $SkipBuild) {
    Push-Location $RustRoot
    try {
        cargo build --release -p wechat-summary-app -p wechat-summary-wxdb -p wechat-summary-gui
    }
    finally {
        Pop-Location
    }
}

$TargetDir = if ($env:CARGO_TARGET_DIR) { $env:CARGO_TARGET_DIR } else { Join-Path $RustRoot "target" }
$ReleaseDir = Join-Path $TargetDir "release"
$AppExe = Join-Path $ReleaseDir "wechat-summary-app.exe"
$WxdbExe = Join-Path $ReleaseDir "wxdb.exe"
$GuiExe = Join-Path $ReleaseDir "wechat-summary-gui.exe"

foreach ($required in @($AppExe, $WxdbExe, $GuiExe)) {
    if (-not (Test-Path $required)) {
        throw "Missing build artifact: $required"
    }
}

$Commit = (git -C $RepoRoot rev-parse --short HEAD).Trim()
$Stamp = Get-Date -Format "yyyyMMdd-HHmmss"
$PackageName = "SummaryAgent4GroupChat-windows-x64-$Stamp-$Commit"
$PackageDir = Join-Path $OutDir $PackageName
$ZipPath = Join-Path $OutDir "$PackageName.zip"

if (Test-Path $PackageDir) {
    Remove-Item -LiteralPath $PackageDir -Recurse -Force
}
if (Test-Path $ZipPath) {
    Remove-Item -LiteralPath $ZipPath -Force
}

New-Item -ItemType Directory -Force -Path `
    (Join-Path $PackageDir "bin"), `
    (Join-Path $PackageDir "config"), `
    (Join-Path $PackageDir "scripts"), `
    (Join-Path $PackageDir "docs"), `
    (Join-Path $PackageDir "runtime") | Out-Null

Copy-Item -LiteralPath $AppExe -Destination (Join-Path $PackageDir "bin\wechat-summary-app.exe")
Copy-Item -LiteralPath $WxdbExe -Destination (Join-Path $PackageDir "bin\wxdb.exe")
Copy-Item -LiteralPath $GuiExe -Destination (Join-Path $PackageDir "bin\wechat-summary-gui.exe")
Copy-Item -LiteralPath $GuiExe -Destination (Join-Path $PackageDir "SummaryAgent4GroupChat.exe")
Copy-Item -LiteralPath (Join-Path $RepoRoot "scripts\wx4py_sidecar.py") -Destination (Join-Path $PackageDir "scripts\wx4py_sidecar.py")
Copy-Item -LiteralPath (Join-Path $RepoRoot ".env.example") -Destination (Join-Path $PackageDir ".env.example")
Copy-Item -LiteralPath (Join-Path $RepoRoot "README.md") -Destination (Join-Path $PackageDir "README-project.md")
Copy-Item -LiteralPath (Join-Path $RustRoot "README.md") -Destination (Join-Path $PackageDir "README-rust-agent.md")
Copy-Item -LiteralPath (Join-Path $RepoRoot "docs\deploy-guide.md") -Destination (Join-Path $PackageDir "docs\deploy-guide.md")

$SourceConfig = Join-Path $RustRoot "config\agent.toml"
$PackageConfig = Join-Path $PackageDir "config\agent.toml"
& (Join-Path $PSScriptRoot "sanitize-agent-config.ps1") `
    -Source $SourceConfig `
    -Destination $PackageConfig
$ConfigText = Get-Content -LiteralPath $PackageConfig -Raw
$ConfigText = $ConfigText -replace 'python_executable\s*=\s*".*"', 'python_executable = ".\\.venv\\Scripts\\python.exe"'
$ConfigText = $ConfigText -replace 'sidecar_script\s*=\s*".*"', 'sidecar_script = ".\\scripts\\wx4py_sidecar.py"'
$ConfigText | Set-Content -LiteralPath $PackageConfig -Encoding UTF8

@'
param(
    [string]$Python = "python"
)

$ErrorActionPreference = "Stop"
$Root = $PSScriptRoot
Set-Location $Root

if (-not (Test-Path ".\.venv\Scripts\python.exe")) {
    & $Python -m venv .venv
}

& ".\.venv\Scripts\python.exe" -m pip install --upgrade pip
& ".\.venv\Scripts\python.exe" -m pip install wx4py

Write-Host "Runtime installed."
Write-Host "Set LLM_API_KEY / LLM_BASE_URL / LLM_MODEL and optional IMAGE_* environment variables before running start.ps1."
'@ | Set-Content -LiteralPath (Join-Path $PackageDir "install.ps1") -Encoding UTF8

@'
$ErrorActionPreference = "Stop"
$Root = $PSScriptRoot
Set-Location $Root
$env:PATH = "$Root\bin;$env:PATH"

& "$Root\bin\wechat-summary-app.exe" --config "$Root\config\agent.toml"
'@ | Set-Content -LiteralPath (Join-Path $PackageDir "start.ps1") -Encoding UTF8

@'
$ErrorActionPreference = "Stop"
$Root = $PSScriptRoot
Set-Location $Root
$env:PATH = "$Root\bin;$env:PATH"

& "$Root\SummaryAgent4GroupChat.exe" --config "$Root\config\agent.toml"
'@ | Set-Content -LiteralPath (Join-Path $PackageDir "start-gui.ps1") -Encoding UTF8

@"
# SummaryAgent4GroupChat Windows Package

Build: $Stamp
Commit: $Commit

## Install runtime

~~~powershell
Set-ExecutionPolicy -Scope Process Bypass
.\install.ps1
~~~

## Configure

Double-click `SummaryAgent4GroupChat.exe` to open the native management UI, or edit `config\agent.toml` directly.

Set these environment variables before launch when the corresponding feature is enabled:

- `LLM_API_KEY`
- `LLM_BASE_URL`
- `LLM_MODEL`
- `IMAGE_API_KEY`
- `IMAGE_BASE_URL`
- `IMAGE_MODEL`
- `DISCORD_BOT_TOKEN` when using Discord

## Run

~~~powershell
.\start.ps1
~~~

## Manage

~~~powershell
.\start-gui.ps1
~~~

The package uses the built-in `wxdb` reader by default, so it does not spawn the external `wx` CLI.
"@ | Set-Content -LiteralPath (Join-Path $PackageDir "README.md") -Encoding UTF8

Compress-Archive -Path (Join-Path $PackageDir "*") -DestinationPath $ZipPath -Force

Write-Host "Package directory: $PackageDir"
Write-Host "Package archive:   $ZipPath"
