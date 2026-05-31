param(
    [string]$OutDir,
    [switch]$SkipBuild
)

$ErrorActionPreference = "Stop"

$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
if ([string]::IsNullOrWhiteSpace($OutDir)) {
    $OutDir = Join-Path $RepoRoot "dist"
}
New-Item -ItemType Directory -Force -Path $OutDir | Out-Null

$Cargo = Get-Command cargo.exe -ErrorAction SilentlyContinue
if (-not $Cargo) {
    throw "cargo.exe not found. Rust is required to build the self-extracting installer."
}

& (Join-Path $PSScriptRoot "package-windows.ps1") -OutDir $OutDir -SkipBuild:$SkipBuild

$PackageZip = Get-ChildItem -LiteralPath $OutDir -Filter "SummaryAgent4GroupChat-windows-x64-*.zip" |
    Sort-Object LastWriteTime -Descending |
    Select-Object -First 1
if (-not $PackageZip) {
    throw "No package zip found in $OutDir"
}

$Commit = (git -C $RepoRoot rev-parse --short HEAD).Trim()
$Stamp = Get-Date -Format "yyyyMMdd-HHmmss"
$InstallerName = "SummaryAgent4GroupChat-Setup-windows-x64-$Stamp-$Commit"
$InstallerPath = Join-Path $OutDir "$InstallerName.exe"

$TempRoot = if (Test-Path "D:\Temp") { "D:\Temp" } else { [System.IO.Path]::GetTempPath() }
$StageDir = Join-Path $TempRoot "summaryagent-rust-installer-$Stamp-$Commit"
$SrcDir = Join-Path $StageDir "src"
$InstallerTargetDir = Join-Path $StageDir "target"

if (Test-Path $StageDir) {
    Remove-Item -LiteralPath $StageDir -Recurse -Force
}
New-Item -ItemType Directory -Force -Path $SrcDir | Out-Null
Copy-Item -LiteralPath $PackageZip.FullName -Destination (Join-Path $SrcDir "payload.zip")

@'
[package]
name = "summaryagent-installer"
version = "0.1.0"
edition = "2021"
publish = false

[profile.release]
opt-level = "z"
lto = true
codegen-units = 1
'@ | Set-Content -LiteralPath (Join-Path $StageDir "Cargo.toml") -Encoding UTF8

$CargoConfigDir = Join-Path $StageDir ".cargo"
New-Item -ItemType Directory -Force -Path $CargoConfigDir | Out-Null
@'
[target.'cfg(windows)']
rustflags = ["-C", "link-arg=/MANIFESTUAC:level='asInvoker' uiAccess='false'"]
'@ | Set-Content -LiteralPath (Join-Path $CargoConfigDir "config.toml") -Encoding UTF8

$MainTemplate = @'
use std::env;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};
use std::time::{SystemTime, UNIX_EPOCH};

const APP_NAME: &str = "SummaryAgent4GroupChat";
const BUILD_STAMP: &str = "__STAMP__";
const COMMIT: &str = "__COMMIT__";
const PAYLOAD: &[u8] = include_bytes!("payload.zip");

#[derive(Debug)]
struct Options {
    quiet: bool,
    install_dir: PathBuf,
}

fn main() -> ExitCode {
    let options = match parse_args() {
        Ok(options) => options,
        Err(message) => {
            eprintln!("{message}");
            print_help();
            return ExitCode::from(2);
        }
    };

    if let Err(error) = install(&options) {
        eprintln!("Install failed: {error}");
        if !options.quiet {
            wait_for_enter();
        }
        return ExitCode::from(1);
    }

    if !options.quiet {
        println!();
        println!("{APP_NAME} installed.");
        println!("Install path: {}", options.install_dir.display());
        println!();
        println!("Next steps:");
        println!("1. Run Install Python Runtime from the Start Menu.");
        println!("2. Edit config\\agent.toml.");
        println!("3. Run Start SummaryAgent4GroupChat.");
        println!();
        println!("Press Enter to close.");
        wait_for_enter();
    }

    ExitCode::SUCCESS
}

fn parse_args() -> Result<Options, String> {
    let mut quiet = false;
    let mut install_dir = env::var_os("SUMMARY_AGENT_INSTALL_DIR").map(PathBuf::from);

    for arg in env::args().skip(1) {
        let lower = arg.to_ascii_lowercase();
        match lower.as_str() {
            "/q" | "/quiet" | "--quiet" => quiet = true,
            "/?" | "/h" | "/help" | "--help" => {
                print_help();
                std::process::exit(0);
            }
            _ if lower.starts_with("/dir=") || lower.starts_with("--dir=") => {
                let value = arg
                    .split_once('=')
                    .map(|(_, value)| value.trim().trim_matches('"'))
                    .unwrap_or_default();
                if value.is_empty() {
                    return Err("Empty install directory.".to_string());
                }
                install_dir = Some(PathBuf::from(value));
            }
            _ => return Err(format!("Unknown argument: {arg}")),
        }
    }

    let install_dir = install_dir.unwrap_or_else(default_install_dir);
    Ok(Options {
        quiet,
        install_dir,
    })
}

fn print_help() {
    println!("{APP_NAME} installer");
    println!();
    println!("Usage:");
    println!("  SummaryAgent4GroupChat-Setup.exe [/quiet] [/dir=C:\\Path\\To\\Install]");
    println!();
    println!("Environment:");
    println!("  SUMMARY_AGENT_INSTALL_DIR overrides the default install directory.");
}

fn default_install_dir() -> PathBuf {
    env::var_os("LOCALAPPDATA")
        .map(PathBuf::from)
        .unwrap_or_else(env::temp_dir)
        .join(APP_NAME)
}

fn install(options: &Options) -> Result<(), Box<dyn std::error::Error>> {
    println!("Installing {APP_NAME} to {}", options.install_dir.display());
    fs::create_dir_all(&options.install_dir)?;

    let config_path = options.install_dir.join("config").join("agent.toml");
    let config_backup = fs::read(&config_path).ok();

    let payload_path = write_payload()?;
    let extract_result = expand_payload(&payload_path, &options.install_dir);
    let _ = fs::remove_file(&payload_path);
    extract_result?;

    if let Some(config) = config_backup {
        if let Some(parent) = config_path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&config_path, config)?;
    }

    write_uninstaller(&options.install_dir)?;
    write_start_menu_entries(&options.install_dir)?;
    write_uninstall_registry(&options.install_dir);

    Ok(())
}

fn write_payload() -> io::Result<PathBuf> {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let path = env::temp_dir().join(format!(
        "{APP_NAME}-payload-{}-{nonce}.zip",
        std::process::id()
    ));
    fs::write(&path, PAYLOAD)?;
    Ok(path)
}

fn expand_payload(payload: &Path, install_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
    match Command::new("tar.exe")
        .arg("-xf")
        .arg(payload)
        .arg("-C")
        .arg(install_dir)
        .status()
    {
        Ok(status) if status.success() => return Ok(()),
        Ok(status) => {
            eprintln!("tar.exe extraction failed with status {status}; falling back to Expand-Archive");
        }
        Err(error) => {
            eprintln!("tar.exe extraction unavailable: {error}; falling back to Expand-Archive");
        }
    }

    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let script_path = env::temp_dir().join(format!(
        "{APP_NAME}-extract-{}-{nonce}.ps1",
        std::process::id()
    ));
    let script = format!(
        "$ErrorActionPreference = 'Stop'\r\nExpand-Archive -LiteralPath '{}' -DestinationPath '{}' -Force\r\n",
        ps_single_quote(&payload.to_string_lossy()),
        ps_single_quote(&install_dir.to_string_lossy())
    );
    let mut script_bytes = vec![0xEF, 0xBB, 0xBF];
    script_bytes.extend_from_slice(script.as_bytes());
    fs::write(&script_path, script_bytes)?;

    let status_result = Command::new("powershell.exe")
        .arg("-NoProfile")
        .arg("-ExecutionPolicy")
        .arg("Bypass")
        .arg("-File")
        .arg(&script_path)
        .status();
    let _ = fs::remove_file(&script_path);
    let status = status_result?;

    if !status.success() {
        return Err(format!("Expand-Archive failed with status {status}").into());
    }
    Ok(())
}

fn write_uninstaller(install_dir: &Path) -> io::Result<()> {
    let install_dir_ps = ps_single_quote(&install_dir.to_string_lossy());
    let script = format!(
        r#"$ErrorActionPreference = "Stop"
$InstallDir = '{install_dir_ps}'
$StartMenuDir = Join-Path $env:APPDATA 'Microsoft\Windows\Start Menu\Programs\SummaryAgent4GroupChat'

Set-Location $env:TEMP

if (Test-Path $StartMenuDir) {{
    Remove-Item -LiteralPath $StartMenuDir -Recurse -Force
}}

reg delete 'HKCU\Software\Microsoft\Windows\CurrentVersion\Uninstall\SummaryAgent4GroupChat' /f 2>$null | Out-Null

if (Test-Path $InstallDir) {{
    Remove-Item -LiteralPath $InstallDir -Recurse -Force
}}

Write-Host 'SummaryAgent4GroupChat removed.'
"#
    );
    fs::write(install_dir.join("uninstall.ps1"), script)?;
    fs::write(
        install_dir.join("uninstall.cmd"),
        "@echo off\r\npowershell.exe -NoProfile -ExecutionPolicy Bypass -File \"%~dp0uninstall.ps1\"\r\nexit /b %ERRORLEVEL%\r\n",
    )?;
    Ok(())
}

fn write_start_menu_entries(install_dir: &Path) -> io::Result<()> {
    let start_menu = env::var_os("APPDATA")
        .map(PathBuf::from)
        .unwrap_or_else(env::temp_dir)
        .join("Microsoft")
        .join("Windows")
        .join("Start Menu")
        .join("Programs")
        .join(APP_NAME);
    fs::create_dir_all(&start_menu)?;

    write_cmd(
        &start_menu.join("Start SummaryAgent4GroupChat.cmd"),
        &format!(
            "powershell.exe -NoProfile -ExecutionPolicy Bypass -File {}",
            cmd_quote(&install_dir.join("start.ps1"))
        ),
    )?;
    write_cmd(
        &start_menu.join("Install Python Runtime.cmd"),
        &format!(
            "powershell.exe -NoProfile -ExecutionPolicy Bypass -File {}",
            cmd_quote(&install_dir.join("install.ps1"))
        ),
    )?;
    write_cmd(
        &start_menu.join("Configure SummaryAgent4GroupChat.cmd"),
        &format!(
            "notepad.exe {}",
            cmd_quote(&install_dir.join("config").join("agent.toml"))
        ),
    )?;
    write_cmd(
        &start_menu.join("Uninstall SummaryAgent4GroupChat.cmd"),
        &format!(
            "powershell.exe -NoProfile -ExecutionPolicy Bypass -File {}",
            cmd_quote(&install_dir.join("uninstall.ps1"))
        ),
    )?;
    Ok(())
}

fn write_cmd(path: &Path, command: &str) -> io::Result<()> {
    fs::write(path, format!("@echo off\r\n{command}\r\nexit /b %ERRORLEVEL%\r\n"))
}

fn write_uninstall_registry(install_dir: &Path) {
    let key = r"HKCU\Software\Microsoft\Windows\CurrentVersion\Uninstall\SummaryAgent4GroupChat";
    let uninstall = format!(
        "powershell.exe -NoProfile -ExecutionPolicy Bypass -File {}",
        cmd_quote(&install_dir.join("uninstall.ps1"))
    );
    let values = [
        ("DisplayName", APP_NAME.to_string()),
        ("DisplayVersion", format!("{BUILD_STAMP}-{COMMIT}")),
        ("Publisher", "fangbm".to_string()),
        ("InstallLocation", install_dir.to_string_lossy().into_owned()),
        ("UninstallString", uninstall.clone()),
        ("QuietUninstallString", uninstall),
    ];

    for (name, value) in values {
        let _ = Command::new("reg")
            .args(["add", key, "/f", "/v", name, "/d", &value])
            .status();
    }
    for name in ["NoModify", "NoRepair"] {
        let _ = Command::new("reg")
            .args(["add", key, "/f", "/v", name, "/t", "REG_DWORD", "/d", "1"])
            .status();
    }
}

fn cmd_quote(path: &Path) -> String {
    format!("\"{}\"", path.to_string_lossy().replace('"', "\\\""))
}

fn ps_single_quote(value: &str) -> String {
    value.replace('\'', "''")
}

fn wait_for_enter() {
    let mut input = String::new();
    let _ = io::stdin().read_line(&mut input);
}
'@

$MainText = $MainTemplate.Replace("__STAMP__", $Stamp).Replace("__COMMIT__", $Commit)
$MainText | Set-Content -LiteralPath (Join-Path $SrcDir "main.rs") -Encoding UTF8

function Add-AsInvokerManifest {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path
    )

    $ManifestXml = @'
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<assembly xmlns="urn:schemas-microsoft-com:asm.v1" manifestVersion="1.0">
  <trustInfo xmlns="urn:schemas-microsoft-com:asm.v3">
    <security>
      <requestedPrivileges>
        <requestedExecutionLevel level="asInvoker" uiAccess="false"/>
      </requestedPrivileges>
    </security>
  </trustInfo>
</assembly>
'@

    if (-not ([System.Management.Automation.PSTypeName]"SummaryAgentResourceUpdater").Type) {
        Add-Type -TypeDefinition @'
using System;
using System.Runtime.InteropServices;

public static class SummaryAgentResourceUpdater
{
    [DllImport("kernel32.dll", SetLastError = true, CharSet = CharSet.Unicode)]
    public static extern IntPtr BeginUpdateResource(string pFileName, bool bDeleteExistingResources);

    [DllImport("kernel32.dll", SetLastError = true)]
    public static extern bool UpdateResource(
        IntPtr hUpdate,
        IntPtr lpType,
        IntPtr lpName,
        ushort wLanguage,
        byte[] lpData,
        uint cbData);

    [DllImport("kernel32.dll", SetLastError = true)]
    public static extern bool EndUpdateResource(IntPtr hUpdate, bool fDiscard);
}
'@
    }

    $Bytes = [System.Text.Encoding]::UTF8.GetBytes($ManifestXml)
    $Handle = [SummaryAgentResourceUpdater]::BeginUpdateResource($Path, $false)
    if ($Handle -eq [IntPtr]::Zero) {
        $ErrorCode = [Runtime.InteropServices.Marshal]::GetLastWin32Error()
        throw "BeginUpdateResource failed for $Path with Win32 error $ErrorCode"
    }

    $Updated = [SummaryAgentResourceUpdater]::UpdateResource(
        $Handle,
        [IntPtr]24,
        [IntPtr]1,
        [UInt16]1033,
        $Bytes,
        [UInt32]$Bytes.Length)
    if (-not $Updated) {
        $ErrorCode = [Runtime.InteropServices.Marshal]::GetLastWin32Error()
        [SummaryAgentResourceUpdater]::EndUpdateResource($Handle, $true) | Out-Null
        throw "UpdateResource failed for $Path with Win32 error $ErrorCode"
    }

    if (-not [SummaryAgentResourceUpdater]::EndUpdateResource($Handle, $false)) {
        $ErrorCode = [Runtime.InteropServices.Marshal]::GetLastWin32Error()
        throw "EndUpdateResource failed for $Path with Win32 error $ErrorCode"
    }
}

$OldCargoTargetDir = $env:CARGO_TARGET_DIR
$env:CARGO_TARGET_DIR = $InstallerTargetDir
try {
    Push-Location $StageDir
    try {
        & $Cargo.Source build --release
        if ($LASTEXITCODE -ne 0) {
            throw "cargo build failed with exit code $LASTEXITCODE"
        }
    }
    finally {
        Pop-Location
    }
}
finally {
    $env:CARGO_TARGET_DIR = $OldCargoTargetDir
}

$BuiltInstaller = Join-Path $InstallerTargetDir "release\summaryagent-installer.exe"
if (-not (Test-Path $BuiltInstaller)) {
    throw "cargo build did not create installer: $BuiltInstaller"
}

Add-AsInvokerManifest -Path $BuiltInstaller
Copy-Item -LiteralPath $BuiltInstaller -Destination $InstallerPath -Force

Write-Host "Source package:    $($PackageZip.FullName)"
Write-Host "Installer archive: $InstallerPath"
