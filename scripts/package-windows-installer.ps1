param(
    [string]$OutDir,
    [switch]$SkipBuild,
    [string]$IsccPath
)

$ErrorActionPreference = "Stop"

$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
if ([string]::IsNullOrWhiteSpace($OutDir)) {
    $OutDir = Join-Path $RepoRoot "dist"
}
New-Item -ItemType Directory -Force -Path $OutDir | Out-Null

function Find-Iscc {
    param(
        [string]$ExplicitPath
    )

    if (-not [string]::IsNullOrWhiteSpace($ExplicitPath)) {
        if (-not (Test-Path -LiteralPath $ExplicitPath)) {
            throw "ISCC.exe not found at $ExplicitPath"
        }
        return (Resolve-Path -LiteralPath $ExplicitPath).Path
    }

    $Command = Get-Command ISCC.exe -ErrorAction SilentlyContinue
    if ($Command) {
        return $Command.Source
    }

    $Candidates = @(
        (Join-Path ${env:ProgramFiles(x86)} "Inno Setup 6\ISCC.exe"),
        (Join-Path $env:ProgramFiles "Inno Setup 6\ISCC.exe"),
        (Join-Path $env:LOCALAPPDATA "Programs\Inno Setup 6\ISCC.exe"),
        (Join-Path ${env:ProgramFiles(x86)} "Inno Setup 5\ISCC.exe"),
        (Join-Path $env:ProgramFiles "Inno Setup 5\ISCC.exe")
    )

    foreach ($Candidate in $Candidates) {
        if (-not [string]::IsNullOrWhiteSpace($Candidate) -and (Test-Path -LiteralPath $Candidate)) {
            return $Candidate
        }
    }

    return $null
}

function Write-Utf8BomFile {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path,
        [Parameter(Mandatory = $true)]
        [string]$Content
    )

    $Utf8Bom = New-Object System.Text.UTF8Encoding($true)
    [System.IO.File]::WriteAllText($Path, $Content, $Utf8Bom)
}

function Escape-InnoString {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Value
    )

    return $Value.Replace("\", "\\").Replace('"', '\"')
}

& (Join-Path $PSScriptRoot "package-windows.ps1") -OutDir $OutDir -SkipBuild:$SkipBuild

$PackageDir = Get-ChildItem -LiteralPath $OutDir -Directory -Filter "SummaryAgent4GroupChat-windows-x64-*" |
    Sort-Object LastWriteTime -Descending |
    Select-Object -First 1
if (-not $PackageDir) {
    throw "No package directory found in $OutDir"
}

$Iscc = Find-Iscc -ExplicitPath $IsccPath
if (-not $Iscc) {
    throw @"
ISCC.exe not found. Install Inno Setup 6 and rerun this script.

Recommended:
  winget install --id JRSoftware.InnoSetup -e --scope user --accept-package-agreements --accept-source-agreements

Or pass:
  .\scripts\package-windows-installer.ps1 -IsccPath "C:\Program Files (x86)\Inno Setup 6\ISCC.exe"
"@
}

$Commit = (git -C $RepoRoot rev-parse --short HEAD).Trim()
$Stamp = Get-Date -Format "yyyyMMdd-HHmmss"
$InstallerBaseName = "SummaryAgent4GroupChat-Inno-Setup-windows-x64-$Stamp-$Commit"
$InstallerPath = Join-Path $OutDir "$InstallerBaseName.exe"
$TempRoot = if (Test-Path "D:\Temp") { "D:\Temp" } else { [System.IO.Path]::GetTempPath() }
$StageDir = Join-Path $TempRoot "summaryagent-inno-installer-$Stamp-$Commit"
$ScriptPath = Join-Path $StageDir "installer.iss"

if (Test-Path -LiteralPath $StageDir) {
    Remove-Item -LiteralPath $StageDir -Recurse -Force
}
New-Item -ItemType Directory -Force -Path $StageDir | Out-Null

$PayloadDir = Escape-InnoString -Value $PackageDir.FullName
$EscapedOutDir = Escape-InnoString -Value $OutDir

$InnoScript = @"
#define MyAppName "SummaryAgent4GroupChat"
#define MyAppPublisher "fangbm"
#define MyAppVersion "$Stamp-$Commit"
#define PayloadDir "$PayloadDir"

[Setup]
AppId={{8E599B3D-9250-43B6-A474-2E95F50290D8}
AppName={#MyAppName}
AppVersion={#MyAppVersion}
AppPublisher={#MyAppPublisher}
AppPublisherURL=https://github.com/fangbm/SummaryAgent4GroupChat
AppSupportURL=https://github.com/fangbm/SummaryAgent4GroupChat
AppUpdatesURL=https://github.com/fangbm/SummaryAgent4GroupChat
DefaultDirName={localappdata}\SummaryAgent4GroupChat
DefaultGroupName=SummaryAgent4GroupChat
DisableProgramGroupPage=yes
OutputDir=$EscapedOutDir
OutputBaseFilename=$InstallerBaseName
Compression=lzma2
SolidCompression=yes
WizardStyle=modern
PrivilegesRequired=admin
ArchitecturesAllowed=x64compatible
UninstallDisplayIcon={app}\SummaryAgent4GroupChat.exe
SetupLogging=yes

[Tasks]
Name: "desktopicon"; Description: "{cm:CreateDesktopIcon}"; GroupDescription: "{cm:AdditionalIcons}"; Flags: unchecked

[Files]
Source: "{#PayloadDir}\*"; DestDir: "{app}"; Flags: ignoreversion recursesubdirs createallsubdirs; Excludes: "config\agent.toml"
Source: "{#PayloadDir}\config\agent.toml"; DestDir: "{app}\config"; Flags: ignoreversion onlyifdoesntexist

[Icons]
Name: "{group}\Manage SummaryAgent4GroupChat"; Filename: "{app}\SummaryAgent4GroupChat.exe"; Parameters: "--config ""{app}\config\agent.toml"""; WorkingDir: "{app}"
Name: "{group}\Start SummaryAgent4GroupChat"; Filename: "{sys}\WindowsPowerShell\v1.0\powershell.exe"; Parameters: "-NoProfile -ExecutionPolicy Bypass -File ""{app}\start.ps1"""; WorkingDir: "{app}"
Name: "{group}\Install Python Runtime"; Filename: "{sys}\WindowsPowerShell\v1.0\powershell.exe"; Parameters: "-NoProfile -ExecutionPolicy Bypass -File ""{app}\install.ps1"""; WorkingDir: "{app}"
Name: "{group}\Configure SummaryAgent4GroupChat"; Filename: "{sys}\notepad.exe"; Parameters: """{app}\config\agent.toml"""; WorkingDir: "{app}"
Name: "{group}\Uninstall SummaryAgent4GroupChat"; Filename: "{uninstallexe}"
Name: "{autodesktop}\SummaryAgent4GroupChat"; Filename: "{app}\SummaryAgent4GroupChat.exe"; Parameters: "--config ""{app}\config\agent.toml"""; WorkingDir: "{app}"; Tasks: desktopicon

[Run]
Filename: "{app}\SummaryAgent4GroupChat.exe"; Parameters: "--config ""{app}\config\agent.toml"""; Description: "Launch SummaryAgent4GroupChat"; Flags: nowait postinstall skipifsilent runascurrentuser

[Code]
procedure StopExistingProcess(ImageName: String);
var
  ResultCode: Integer;
begin
  Exec(ExpandConstant('{sys}\taskkill.exe'), '/IM "' + ImageName + '" /T /F', '', SW_HIDE, ewWaitUntilTerminated, ResultCode);
end;

procedure CurStepChanged(CurStep: TSetupStep);
begin
  if CurStep = ssInstall then
  begin
    StopExistingProcess('wechat-summary-app.exe');
    StopExistingProcess('wechat-summary-gui.exe');
    StopExistingProcess('SummaryAgent4GroupChat.exe');
  end;
end;
"@

Write-Utf8BomFile -Path $ScriptPath -Content $InnoScript

& $Iscc $ScriptPath
if ($LASTEXITCODE -ne 0) {
    throw "ISCC failed with exit code $LASTEXITCODE"
}

if (-not (Test-Path -LiteralPath $InstallerPath)) {
    throw "Inno Setup did not create installer: $InstallerPath"
}

Write-Host "Package directory: $($PackageDir.FullName)"
Write-Host "Inno compiler:    $Iscc"
Write-Host "Installer:        $InstallerPath"
