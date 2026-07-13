param(
    [Parameter(Mandatory = $true)][string]$Source,
    [Parameter(Mandatory = $true)][string]$Destination
)

$ErrorActionPreference = "Stop"

function Test-SensitiveConfigKey([string]$Key) {
    $normalized = [regex]::Replace($Key.ToLowerInvariant(), '[^a-z0-9]', '')
    if ($normalized.EndsWith('env') -or $normalized.EndsWith('envvar')) {
        return $false
    }
    foreach ($marker in @(
        'apikey', 'accesstoken', 'refreshtoken', 'idtoken', 'token',
        'password', 'passwd', 'clientsecret', 'secret', 'authorization',
        'credential', 'privatekey'
    )) {
        if ($normalized.Contains($marker)) {
            return $true
        }
    }
    return $false
}

# TOML keys may be bare, double-quoted, or single-quoted, including dotted keys.
$tomlKeySegmentPattern = '(?:"(?:\\.|[^"\\])*"|''(?:''''|[^''])*''|[A-Za-z0-9_-]+)'
$tomlKeyPattern = "$tomlKeySegmentPattern(?:\s*\.\s*$tomlKeySegmentPattern)*"
$sensitiveMarkerPattern = 'api[_-]?key|access[_-]?token|refresh[_-]?token|id[_-]?token|token|password|passwd|client[_-]?secret|secret|authorization|credential|private[_-]?key'
$bareSensitiveKeyPattern = "[A-Za-z0-9_.-]*(?:$sensitiveMarkerPattern)[A-Za-z0-9_.-]*"
$doubleQuotedSensitiveKeyPattern = '"(?:\\.|[^"\\])*(?:' + $sensitiveMarkerPattern + ')(?:\\.|[^"\\])*"'
$singleQuotedSensitiveKeyPattern = "'(?:''|[^'])*(?:$sensitiveMarkerPattern)(?:''|[^'])*'"
$sensitiveKeyPattern = "(?:$bareSensitiveKeyPattern|$doubleQuotedSensitiveKeyPattern|$singleQuotedSensitiveKeyPattern)"

$output = [System.Collections.Generic.List[string]]::new()
foreach ($line in Get-Content -LiteralPath $Source) {
    if ($line -match '^\s*#') {
        $output.Add($line)
        continue
    }
    if ($line -match "^\s*(?<key>$tomlKeyPattern)\s*=") {
        if (Test-SensitiveConfigKey $Matches.key) {
            continue
        }
    }
    $output.Add($line)
}

$sanitized = $output -join "`r`n"

# Remove secrets inside inline TOML tables as well as top-level assignments.
# Unsupported assignments are caught by the validation pass below.
$inlineSensitivePattern = '(?im)(?<open>[{,]\s*)(?<key>' + $sensitiveKeyPattern + ')\s*=\s*(?<value>"(?:\\.|[^"\\])*"|''(?:''''|[^''])*''|[^,}\r\n]+)(?<comma>\s*,?)'
$sanitized = [regex]::Replace($sanitized, $inlineSensitivePattern, {
    param($match)
    if (-not (Test-SensitiveConfigKey $match.Groups['key'].Value)) {
        return $match.Value
    }

    $open = $match.Groups['open'].Value
    if ($match.Groups['comma'].Value.Contains(',')) {
        return $open
    }
    if ($open.TrimStart().StartsWith(',')) {
        return ''
    }
    return $open
})

foreach ($line in $sanitized -split "`r?`n") {
    if ($line -match '^\s*#') {
        continue
    }
    $matches = [regex]::Matches(
        $line,
        "(?i)(?<key>$sensitiveKeyPattern)\s*="
    )
    foreach ($match in $matches) {
        if (Test-SensitiveConfigKey $match.Groups['key'].Value) {
            throw "Refusing to package config; sensitive assignment remains: $($match.Groups['key'].Value)"
        }
    }
}

$parent = Split-Path -Parent $Destination
if ($parent) {
    New-Item -ItemType Directory -Force -Path $parent | Out-Null
}
$sanitized | Set-Content -LiteralPath $Destination -Encoding UTF8
