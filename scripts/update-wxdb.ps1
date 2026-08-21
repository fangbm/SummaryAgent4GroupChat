<#
.SYNOPSIS
    Updates only the external wxdb tool: download latest release, verify its
    SHA256, replace the managed copy, and point the agent config at it.

    Deliberately separate from install-python-runtime.ps1 so updating wxdb
    does not re-run Python/venv setup or wxdb init.
#>
param(
    [string]$RootPath = "",
    [string]$ConfigPath,
    [string]$WxdbDownloadUrl = "",
    [string]$WxdbExpectedSha256 = ""
)

$ErrorActionPreference = "Stop"
$ProgressPreference = "SilentlyContinue"
# Windows PowerShell 5.1 otherwise writes Chinese status text using the active
# ANSI code page when its output is captured by the Rust control service.
$Utf8OutputEncoding = New-Object System.Text.UTF8Encoding($false)
[Console]::InputEncoding = $Utf8OutputEncoding
[Console]::OutputEncoding = $Utf8OutputEncoding
$OutputEncoding = $Utf8OutputEncoding

$WxdbReleaseRepository = "fangbm/wxdb"
# Keep a known-good direct asset URL as a fallback. GitHub's unauthenticated
# REST API is shared per public IP and can return 403 after its rate limit.
$FallbackWxdbDownloadUrl = "https://github.com/fangbm/wxdb/releases/download/v0.1.0/wxdb-v0.1.0-windows-x64.zip"

function Write-Step {
    param([string]$Message)
    Write-Host "[wxdb-update] $Message"
}

function Get-LatestWxdbDownloadUrl {
    try {
        # The releases page does not consume the REST API rate limit. It redirects
        # to /releases/tag/<tag>, from which the versioned Windows asset name is stable.
        $latest = Invoke-WebRequest `
            -Uri "https://github.com/$WxdbReleaseRepository/releases/latest" `
            -Headers @{ "User-Agent" = "SummaryAgent4GroupChat wxdb updater" } `
            -UseBasicParsing
        $path = $latest.BaseResponse.ResponseUri.AbsolutePath
        $match = [regex]::Match($path, '/releases/tag/(?<tag>[^/]+)$')
        if ($match.Success) {
            $tag = [uri]::UnescapeDataString($match.Groups['tag'].Value)
            return "https://github.com/$WxdbReleaseRepository/releases/download/$tag/wxdb-$tag-windows-x64.zip"
        }
        Write-Warning "未能识别 wxdb 最新版本标签，改用已验证的下载地址。"
    }
    catch {
        Write-Warning "读取 wxdb 最新版本失败，改用已验证的下载地址：$($_.Exception.Message)"
    }
    return $FallbackWxdbDownloadUrl
}

function Get-ExpectedWxdbSha256 {
    param(
        [string]$DownloadUrl,
        [string]$ExpectedSha256,
        [string]$DownloadDir
    )

    if (-not [string]::IsNullOrWhiteSpace($ExpectedSha256)) {
        return $ExpectedSha256.Trim().ToLowerInvariant()
    }

    # Optional sidecar published next to the asset: <url>.sha256 containing
    # either the bare digest or "<digest>  <filename>".
    $sidecarPath = Join-Path $DownloadDir "wxdb.zip.sha256"
    try {
        Invoke-WebRequest `
            -Uri "$DownloadUrl.sha256" `
            -OutFile $sidecarPath `
            -Headers @{ "User-Agent" = "SummaryAgent4GroupChat wxdb updater" } `
            -UseBasicParsing
        $text = (Get-Content -LiteralPath $sidecarPath -Raw).Trim()
        if ($text -match '\b[0-9a-fA-F]{64}\b') {
            return $Matches[0].ToLowerInvariant()
        }
    }
    catch {
        # Sidecar may not exist yet; caller falls back to a warning.
    }
    return $null
}

function Get-RelativeConfigPath {
    param(
        [string]$FromDirectory,
        [string]$ToPath
    )

    $fromPath = [System.IO.Path]::GetFullPath($FromDirectory).TrimEnd([System.IO.Path]::DirectorySeparatorChar)
    $from = New-Object System.Uri ($fromPath + [System.IO.Path]::DirectorySeparatorChar)
    $to = New-Object System.Uri ([System.IO.Path]::GetFullPath($ToPath))
    $relative = [System.Uri]::UnescapeDataString($from.MakeRelativeUri($to).ToString()).Replace('/', '\')
    if ($relative.StartsWith('.')) {
        return $relative
    }
    return ".\\$relative"
}

function Set-WxdbExecutableInConfig {
    param(
        [string]$Path,
        [string]$WxdbExecutable
    )

    if (-not (Test-Path -LiteralPath $Path)) {
        throw "配置文件不存在：$Path"
    }

    $text = [System.IO.File]::ReadAllText($Path)
    $text = [regex]::Replace(
        $text,
        '(?ms)(\[wxdb\].*?^executable\s*=\s*)"[^"]*"',
        ('${1}"' + $WxdbExecutable.Replace('\', '\\') + '"')
    )
    $utf8NoBom = New-Object System.Text.UTF8Encoding($false)
    [System.IO.File]::WriteAllText($Path, $text, $utf8NoBom)
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

$target = Join-Path $RootPath "tools\wxdb\wxdb.exe"
$downloadDir = Join-Path $env:TEMP "SummaryAgent4GroupChat-wxdb-update"
$archive = Join-Path $downloadDir "wxdb.zip"
$extractDir = Join-Path $downloadDir "extract"
Remove-Item -LiteralPath $downloadDir -Recurse -Force -ErrorAction SilentlyContinue
New-Item -ItemType Directory -Force -Path $downloadDir, (Split-Path -Parent $target) | Out-Null

try {
    if ([string]::IsNullOrWhiteSpace($WxdbDownloadUrl)) {
        $WxdbDownloadUrl = Get-LatestWxdbDownloadUrl
    }
    Write-Step "正在从独立 wxdb Release 下载最新版本..."
    Invoke-WebRequest `
        -Uri $WxdbDownloadUrl `
        -OutFile $archive `
        -Headers @{ "User-Agent" = "SummaryAgent4GroupChat wxdb updater" } `
        -UseBasicParsing

    $expectedHash = Get-ExpectedWxdbSha256 `
        -DownloadUrl $WxdbDownloadUrl `
        -ExpectedSha256 $WxdbExpectedSha256 `
        -DownloadDir $downloadDir
    if ($expectedHash) {
        $actualHash = (Get-FileHash -LiteralPath $archive -Algorithm SHA256).Hash.ToLowerInvariant()
        if ($actualHash -ne $expectedHash) {
            throw "wxdb 压缩包 SHA256 校验失败（期望 $expectedHash，实际 $actualHash），已中止更新。"
        }
        Write-Step "wxdb 压缩包 SHA256 校验通过。"
    }
    else {
        Write-Warning "wxdb Release 未提供 SHA256 校验值，本次下载跳过完整性校验。建议在 wxdb Release 中附带 .sha256 文件。"
    }

    Expand-Archive -LiteralPath $archive -DestinationPath $extractDir -Force
    $downloaded = Get-ChildItem -LiteralPath $extractDir -Filter "wxdb.exe" -Recurse | Select-Object -First 1
    if (-not $downloaded) {
        throw "压缩包中未找到 wxdb.exe"
    }
    Copy-Item -LiteralPath $downloaded.FullName -Destination $target -Force
    Write-Step "wxdb 已更新到：$target"

    $configWxdbPath = Get-RelativeConfigPath -FromDirectory $ConfigBasePath -ToPath $target
    Set-WxdbExecutableInConfig -Path $ConfigPath -WxdbExecutable $configWxdbPath
    Write-Step "已更新运行环境配置：$ConfigPath"

    $versionOutput = & $target --version 2>$null
    if ($LASTEXITCODE -eq 0 -and -not [string]::IsNullOrWhiteSpace($versionOutput)) {
        Write-Step "当前 wxdb 版本：$($versionOutput | Select-Object -First 1)"
    }

    Write-Step "wxdb 更新完成。如需刷新密钥缓存，请在微信登录后运行 wxdb init。"
}
catch {
    throw "无法更新独立 wxdb 运行时：$($_.Exception.Message)。下载地址：$WxdbDownloadUrl。请检查网络访问 GitHub Release；也可手动下载后替换 $target。"
}
finally {
    Remove-Item -LiteralPath $downloadDir -Recurse -Force -ErrorAction SilentlyContinue
}
