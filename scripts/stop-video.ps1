$ErrorActionPreference = "Stop"
$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$pidFile = Join-Path $projectRoot ".codex_tmp\video-dashboard.pid"

function Stop-ProjectDashboardProcesses {
    $escapedRoot = [regex]::Escape($projectRoot)
    Get-CimInstance Win32_Process |
        Where-Object {
            $_.CommandLine -and
            $_.CommandLine -match $escapedRoot -and
            ($_.CommandLine -match 'vinext\\dist\\cli\.js" dev' -or $_.CommandLine -match 'npm-cli\.js" run dev')
        } |
        ForEach-Object {
            Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue
        }
}

function Stop-ProjectVideoApiProcesses {
    $escapedRoot = [regex]::Escape($projectRoot)
    Get-CimInstance Win32_Process |
        Where-Object {
            $_.CommandLine -and
            $_.CommandLine -match $escapedRoot -and
            $_.CommandLine -match "services\.video_producer\.app"
        } |
        ForEach-Object {
            Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue
        }
}

if (Test-Path $pidFile) {
    $dashboardPid = [int](Get-Content -LiteralPath $pidFile -Raw)
    $dashboardProcess = Get-Process -Id $dashboardPid -ErrorAction SilentlyContinue
    if ($dashboardProcess) {
        Stop-Process -Id $dashboardPid -Force
    }
    Remove-Item -LiteralPath $pidFile -Force
}

Stop-ProjectDashboardProcesses
Stop-ProjectVideoApiProcesses

Write-Host "Video dashboard and API stopped." -ForegroundColor Yellow
