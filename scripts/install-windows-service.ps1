param(
    [string]$Python = "python",
    [string]$Config = "config\worker.yaml"
)

Write-Host "Install with NSSM or Windows Task Scheduler using:"
Write-Host "$Python -m windows_worker.main --config $Config --mode single"
