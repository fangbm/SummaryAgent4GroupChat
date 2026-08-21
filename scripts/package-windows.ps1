param(
    [string]$OutDir,
    [switch]$SkipBuild
)

$ErrorActionPreference = "Stop"

$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$RustRoot = Join-Path $RepoRoot "rust-agent"
$WinUiProject = Join-Path $RepoRoot "windows-ui\src\SummaryAgent4GroupChat.WinUI\SummaryAgent4GroupChat.WinUI.csproj"
$WinUiPublishDir = Join-Path $RepoRoot ".artifacts\winui-publish"
$BuildCacheDir = Join-Path $RepoRoot ".artifacts\build-cache"
if ([string]::IsNullOrWhiteSpace($OutDir)) {
    $OutDir = Join-Path $RepoRoot "dist"
}

# Self-contained .NET publish can download several hundred MB of runtime packs.
# Keep transient restore data beside the project rather than filling the system drive.
New-Item -ItemType Directory -Force -Path $BuildCacheDir | Out-Null
$env:NUGET_PACKAGES = Join-Path $BuildCacheDir "nuget-packages"
$env:TEMP = Join-Path $BuildCacheDir "temp"
$env:TMP = $env:TEMP
New-Item -ItemType Directory -Force -Path $env:NUGET_PACKAGES, $env:TEMP | Out-Null

if (-not $SkipBuild) {
    Push-Location $RustRoot
    try {
        cargo build --release -p wechat-summary-app -p wechat-summary-control -p wechat-summary-gui
    }
    finally {
        Pop-Location
    }
}

if (-not $SkipBuild) {
    if (Test-Path -LiteralPath $WinUiPublishDir) {
        Remove-Item -LiteralPath $WinUiPublishDir -Recurse -Force
    }
    dotnet publish $WinUiProject -c Release -r win-x64 --self-contained true -p:Platform=x64 -o $WinUiPublishDir
}

$TargetDir = if ($env:CARGO_TARGET_DIR) { $env:CARGO_TARGET_DIR } else { Join-Path $RustRoot "target" }
$ReleaseDir = Join-Path $TargetDir "release"
$AppExe = Join-Path $ReleaseDir "wechat-summary-app.exe"
$GuiExe = Join-Path $ReleaseDir "wechat-summary-gui.exe"
$ControlExe = Join-Path $ReleaseDir "wechat-summary-control.exe"
$WinUiExe = Join-Path $WinUiPublishDir "SummaryAgent4GroupChat.exe"

foreach ($required in @($AppExe, $GuiExe, $ControlExe, $WinUiExe)) {
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
Copy-Item -LiteralPath $ControlExe -Destination (Join-Path $PackageDir "bin\wechat-summary-control.exe")
Copy-Item -LiteralPath $GuiExe -Destination (Join-Path $PackageDir "bin\SummaryAgent4GroupChat.Legacy.exe")
Copy-Item -Path (Join-Path $WinUiPublishDir "*") -Destination $PackageDir -Recurse -Force
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
$ConfigText = Get-Content -LiteralPath $PackageConfig -Raw -Encoding UTF8
$ConfigText = $ConfigText -replace 'python_executable\s*=\s*".*"', 'python_executable = ".\\.venv\\Scripts\\python.exe"'
$ConfigText = $ConfigText -replace 'sidecar_script\s*=\s*".*"', 'sidecar_script = ".\\scripts\\wx4py_sidecar.py"'
# Write UTF-8 without BOM so Chinese group names survive on Windows PowerShell 5.1.
$utf8NoBom = New-Object System.Text.UTF8Encoding($false)
[System.IO.File]::WriteAllText($PackageConfig, $ConfigText, $utf8NoBom)

# Windows PowerShell 5.1 treats a BOM-less script as the current ANSI code page.
# Keep the packaged script BOM-marked so its Chinese status and error messages parse correctly.
$RuntimeInstallerSource = Get-Content -LiteralPath (Join-Path $PSScriptRoot "install-python-runtime.ps1") -Raw -Encoding UTF8
$RuntimeInstallerEncoding = New-Object System.Text.UTF8Encoding($true)
[System.IO.File]::WriteAllText(
    (Join-Path $PackageDir "install.ps1"),
    $RuntimeInstallerSource,
    $RuntimeInstallerEncoding
)

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

## Install or repair the WeChat runtime

~~~powershell
Set-ExecutionPolicy -Scope Process Bypass
.\install.ps1
~~~

The installer detects Python 3.11/3.12, installs Python 3.12 with `winget` when
needed, creates `.venv`, installs `wx4py`, downloads the separately released
`wxdb` runtime, updates `config\agent.toml`, and then attempts `wxdb init`.
The application package does not contain wxdb source code or database logic.

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

Multi-key concurrency (optional): set the `*_API_KEYS` variables
(`LLM_API_KEYS`, `IMAGE_API_KEYS`, `IMAGE_CAPTION_API_KEYS`,
`VIDEO_CAPTION_API_KEYS`, `VOICE_TRANSCRIPTION_API_KEYS`) to a comma/newline
separated key list, or fill `api_keys` in `config\agent.toml`. Add
`max_concurrent_per_key` in each section to cap per-account concurrency
(`0` = unlimited, the default).

## Run

~~~powershell
.\start.ps1
~~~

## Manage

~~~powershell
.\start-gui.ps1
~~~

The package does not include a database reader. Install and configure a compatible external history provider separately, then set `[wxdb].executable` to its path.
"@ | Set-Content -LiteralPath (Join-Path $PackageDir "README.md") -Encoding UTF8

Add-Type -AssemblyName System.IO.Compression.FileSystem
[System.IO.Compression.ZipFile]::CreateFromDirectory(
    $PackageDir,
    $ZipPath,
    [System.IO.Compression.CompressionLevel]::Optimal,
    $false
)

Write-Host "Package directory: $PackageDir"
Write-Host "Package archive:   $ZipPath"
