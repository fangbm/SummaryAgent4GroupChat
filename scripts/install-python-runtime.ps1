param(
    [string]$RootPath = "",
    [string]$ConfigPath,
    [string]$ExistingWxdbExecutable = "",
    [string]$WxdbDownloadUrl = "",
    [switch]$ForceWxdbUpdate,
    [switch]$SkipWxdbInit
)

$ErrorActionPreference = "Stop"
$ProgressPreference = "SilentlyContinue"
# Windows PowerShell 5.1 otherwise writes Chinese status and error text using
# the active ANSI code page when its output is captured by the Rust GUI.
$Utf8OutputEncoding = New-Object System.Text.UTF8Encoding($false)
[Console]::InputEncoding = $Utf8OutputEncoding
[Console]::OutputEncoding = $Utf8OutputEncoding
$OutputEncoding = $Utf8OutputEncoding
$env:PYTHONUTF8 = "1"
$env:PYTHONIOENCODING = "utf-8"

$WxdbReleaseRepository = "fangbm/wxdb"
# Keep a known-good direct asset URL as a fallback. GitHub's unauthenticated
# REST API is shared per public IP and can return 403 after its rate limit.
$FallbackWxdbDownloadUrl = "https://github.com/fangbm/wxdb/releases/download/v0.1.0/wxdb-v0.1.0-windows-x64.zip"

function Write-Step {
    param([string]$Message)
    Write-Host "[setup] $Message"
}

function Get-PythonCommand {
    $candidates = @(
        @("py.exe", @("-3.12")),
        @("py.exe", @("-3.11")),
        @("python.exe", @())
    )

    foreach ($candidate in $candidates) {
        $command = Get-Command $candidate[0] -ErrorAction SilentlyContinue
        if (-not $command) {
            continue
        }
        try {
            $version = & $command.Source @($candidate[1]) -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')" 2>$null
            if ($LASTEXITCODE -eq 0 -and $version -match '^(3\.11|3\.12)$') {
                return [PSCustomObject]@{
                    FilePath = $command.Source
                    Arguments = @($candidate[1])
                    Version = $version.Trim()
                }
            }
        }
        catch {
            continue
        }
    }
    return $null
}

function Install-Python {
    $winget = Get-Command winget.exe -ErrorAction SilentlyContinue
    if (-not $winget) {
        throw "未检测到 Python 3.11/3.12，且系统没有 winget。请先安装 Python 3.12 并重新运行此安装程序。"
    }
    Write-Step "未检测到兼容 Python，正在通过 winget 安装 Python 3.12..."
    & $winget.Source install --id Python.Python.3.12 --exact --scope user --accept-package-agreements --accept-source-agreements --disable-interactivity
    if ($LASTEXITCODE -ne 0) {
        throw "winget 安装 Python 3.12 失败，退出码 $LASTEXITCODE"
    }
}

function Update-AgentConfig {
    param(
        [string]$Path,
        [string]$PythonExecutable,
        [string]$SidecarScript,
        [string]$WxdbExecutable,
        [string]$CacheDir
    )

    if (-not (Test-Path -LiteralPath $Path)) {
        throw "配置文件不存在：$Path"
    }

    $text = [System.IO.File]::ReadAllText($Path)
    $text = [regex]::Replace(
        $text,
        '(?ms)(\[wx4py\].*?^python_executable\s*=\s*)"[^"]*"',
        ('${1}"' + $PythonExecutable.Replace('\', '\\') + '"')
    )
    $text = [regex]::Replace(
        $text,
        '(?ms)(\[wx4py\].*?^sidecar_script\s*=\s*)"[^"]*"',
        ('${1}"' + $SidecarScript.Replace('\', '\\') + '"')
    )
    $text = [regex]::Replace(
        $text,
        '(?ms)(\[wxdb\].*?^executable\s*=\s*)"[^"]*"',
        ('${1}"' + $WxdbExecutable.Replace('\', '\\') + '"')
    )
    $text = [regex]::Replace(
        $text,
        '(?ms)(\[wxdb\].*?^cache_dir\s*=\s*)""',
        ('${1}"' + $CacheDir.Replace('\', '\\') + '"')
    )
    $utf8NoBom = New-Object System.Text.UTF8Encoding($false)
    [System.IO.File]::WriteAllText($Path, $text, $utf8NoBom)
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

function Get-LatestWxdbDownloadUrl {
    try {
        # The releases page does not consume the REST API rate limit. It redirects
        # to /releases/tag/<tag>, from which the versioned Windows asset name is stable.
        $latest = Invoke-WebRequest `
            -Uri "https://github.com/$WxdbReleaseRepository/releases/latest" `
            -Headers @{ "User-Agent" = "SummaryAgent4GroupChat runtime installer" } `
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

function Get-WxdbExecutable {
    param(
        [string]$InstallRoot,
        [string]$ExistingExecutable,
        [string]$DownloadUrl,
        [switch]$ForceUpdate
    )

    if (-not $ForceUpdate -and -not [string]::IsNullOrWhiteSpace($ExistingExecutable) -and (Test-Path -LiteralPath $ExistingExecutable)) {
        $existingPath = (Resolve-Path -LiteralPath $ExistingExecutable).Path
        Write-Step "已检测到配置的 wxdb：$existingPath"
        return $existingPath
    }

    $target = Join-Path $InstallRoot "tools\wxdb\wxdb.exe"
    if (-not $ForceUpdate -and (Test-Path -LiteralPath $target)) {
        Write-Step "已检测到 wxdb：$target"
        return $target
    }

    $existing = if ($ForceUpdate) { $null } else { Get-Command wxdb.exe -ErrorAction SilentlyContinue }
    if ($existing) {
        Write-Step "检测到 PATH 中的 wxdb：$($existing.Source)"
        return $existing.Source
    }

    $downloadDir = Join-Path $env:TEMP "SummaryAgent4GroupChat-wxdb"
    $archive = Join-Path $downloadDir "wxdb.zip"
    $extractDir = Join-Path $downloadDir "extract"
    Remove-Item -LiteralPath $downloadDir -Recurse -Force -ErrorAction SilentlyContinue
    New-Item -ItemType Directory -Force -Path $downloadDir, (Split-Path -Parent $target) | Out-Null

    try {
        if ([string]::IsNullOrWhiteSpace($DownloadUrl)) {
            $DownloadUrl = Get-LatestWxdbDownloadUrl
        }
        Write-Step "正在从独立 wxdb Release 下载运行时..."
        Invoke-WebRequest `
            -Uri $DownloadUrl `
            -OutFile $archive `
            -Headers @{ "User-Agent" = "SummaryAgent4GroupChat runtime installer" } `
            -UseBasicParsing
        Expand-Archive -LiteralPath $archive -DestinationPath $extractDir -Force
        $downloaded = Get-ChildItem -LiteralPath $extractDir -Filter "wxdb.exe" -Recurse | Select-Object -First 1
        if (-not $downloaded) {
            throw "压缩包中未找到 wxdb.exe"
        }
        Copy-Item -LiteralPath $downloaded.FullName -Destination $target -Force
        Write-Step "wxdb 已安装到：$target"
        return $target
    }
    catch {
        throw "无法安装独立 wxdb 运行时：$($_.Exception.Message)。下载地址：$DownloadUrl。请检查网络访问 GitHub Release；也可在接入平台页填写 wxdb.exe 的本地绝对路径后再次运行安装。"
    }
    finally {
        Remove-Item -LiteralPath $downloadDir -Recurse -Force -ErrorAction SilentlyContinue
    }
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

Write-Step "检查 Python 3.11/3.12..."
$python = Get-PythonCommand
if (-not $python) {
    Install-Python
    $python = Get-PythonCommand
}
if (-not $python) {
    throw "Python 安装完成后仍未检测到 Python 3.11/3.12；请关闭并重新打开 SummaryAgent4GroupChat 后重试。"
}

$venvPython = Join-Path $RootPath ".venv\Scripts\python.exe"
if (-not (Test-Path -LiteralPath $venvPython)) {
    Write-Step "使用 Python $($python.Version) 创建隔离运行环境..."
    & $python.FilePath @($python.Arguments) -m venv (Join-Path $RootPath ".venv")
    if ($LASTEXITCODE -ne 0) {
        throw "创建 Python 虚拟环境失败，退出码 $LASTEXITCODE"
    }
}

Write-Step "检查 pip..."
& $venvPython -m pip --version
if ($LASTEXITCODE -ne 0) { throw "pip 不可用，退出码 $LASTEXITCODE" }

Write-Step "安装 wx4py..."
& $venvPython -m pip install --disable-pip-version-check wx4py
if ($LASTEXITCODE -ne 0) { throw "安装 wx4py 失败，退出码 $LASTEXITCODE" }

$wxdb = Get-WxdbExecutable `
    -InstallRoot $RootPath `
    -ExistingExecutable $ExistingWxdbExecutable `
    -DownloadUrl $WxdbDownloadUrl `
    -ForceUpdate:$ForceWxdbUpdate
$cacheDir = Join-Path $ConfigBasePath "runtime\wxdb-cache"
$configPythonPath = Get-RelativeConfigPath -FromDirectory $ConfigBasePath -ToPath $venvPython
$configSidecarPath = Get-RelativeConfigPath -FromDirectory $ConfigBasePath -ToPath (Join-Path $RootPath "scripts\wx4py_sidecar.py")
$configWxdbPath = Get-RelativeConfigPath -FromDirectory $ConfigBasePath -ToPath $wxdb
$configCachePath = Get-RelativeConfigPath -FromDirectory $ConfigBasePath -ToPath $cacheDir
Update-AgentConfig `
    -Path $ConfigPath `
    -PythonExecutable $configPythonPath `
    -SidecarScript $configSidecarPath `
    -WxdbExecutable $configWxdbPath `
    -CacheDir $configCachePath
New-Item -ItemType Directory -Force -Path $cacheDir | Out-Null
Write-Step "已更新运行环境配置：$ConfigPath"

if (-not $SkipWxdbInit) {
    Write-Step "正在初始化 wxdb 密钥缓存..."
    & $wxdb init
    if ($LASTEXITCODE -ne 0) {
        Write-Warning "wxdb init 未完成（退出码 $LASTEXITCODE）。请确认微信已登录，然后在 GUI 中点击运行外部 wxdb init 重试。"
    }
}

Write-Step "安装完成。"
