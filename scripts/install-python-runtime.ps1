param(
    [string]$RootPath = "",
    [string]$ConfigPath,
    [string]$ExistingWxdbExecutable = "",
    [string]$WxdbDownloadUrl = "",
    [switch]$SkipWxdbInit
)

$ErrorActionPreference = "Stop"
$ProgressPreference = "SilentlyContinue"

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

function Get-WxdbExecutable {
    param(
        [string]$InstallRoot,
        [string]$ExistingExecutable,
        [string]$DownloadUrl
    )

    if (-not [string]::IsNullOrWhiteSpace($ExistingExecutable) -and (Test-Path -LiteralPath $ExistingExecutable)) {
        $existingPath = (Resolve-Path -LiteralPath $ExistingExecutable).Path
        Write-Step "已检测到配置的 wxdb：$existingPath"
        return $existingPath
    }

    $target = Join-Path $InstallRoot "tools\wxdb\wxdb.exe"
    if (Test-Path -LiteralPath $target) {
        Write-Step "已检测到 wxdb：$target"
        return $target
    }

    $existing = Get-Command wxdb.exe -ErrorAction SilentlyContinue
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
            $release = Invoke-RestMethod -Uri "https://api.github.com/repos/fangbm/wxdb/releases/latest"
            $asset = @($release.assets) | Where-Object {
                $_.name -match '^wxdb-v.+-windows-x64\.zip$'
            } | Select-Object -First 1
            if (-not $asset) {
                throw "最新 wxdb Release 中没有 Windows x64 压缩包"
            }
            $DownloadUrl = $asset.browser_download_url
        }
        Write-Step "正在从独立 wxdb Release 下载运行时..."
        Invoke-WebRequest -Uri $DownloadUrl -OutFile $archive -UseBasicParsing
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
        throw "无法安装独立 wxdb 运行时：$($_.Exception.Message)。请在 wxdb 项目发布 Windows Release 后重试，或将 wxdb.exe 加入 PATH 后再次运行安装。"
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

Write-Step "升级 pip..."
& $venvPython -m pip install --upgrade pip
if ($LASTEXITCODE -ne 0) { throw "升级 pip 失败，退出码 $LASTEXITCODE" }

Write-Step "安装 wx4py..."
& $venvPython -m pip install --upgrade wx4py
if ($LASTEXITCODE -ne 0) { throw "安装 wx4py 失败，退出码 $LASTEXITCODE" }

$wxdb = Get-WxdbExecutable `
    -InstallRoot $RootPath `
    -ExistingExecutable $ExistingWxdbExecutable `
    -DownloadUrl $WxdbDownloadUrl
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
        Write-Warning "wxdb init 未完成（退出码 $LASTEXITCODE）。请确认微信已登录，然后在 GUI 中点击“运行外部 wxdb init”重试。"
    }
}

Write-Step "安装完成。"
