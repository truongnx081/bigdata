param(
    [string]$Source,
    [string]$RoadName = "Nguon video truc tiep",
    [double]$Latitude = 10.7732,
    [double]$Longitude = 106.7035,
    [switch]$Loop,
    [switch]$Preview,
    [switch]$NoRealtime
)

$ErrorActionPreference = "Stop"
$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
Set-Location $projectRoot

function Stop-ProjectDashboardProcesses {
    $escapedRoot = [regex]::Escape($projectRoot)
    Get-CimInstance Win32_Process |
        Where-Object {
            $_.CommandLine -and
            $_.CommandLine -match $escapedRoot -and
            ($_.CommandLine -match 'vinext' -or $_.CommandLine -match 'npm-cli\.js' -or $_.CommandLine -match 'npm\.cmd') -and
            $_.CommandLine -match 'dev'
        } |
        ForEach-Object {
            Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue
        }
}

function Get-PortOwnerText {
    param([int]$Port)

    try {
        $connection = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction Stop | Select-Object -First 1
        if ($connection) {
            $process = Get-Process -Id $connection.OwningProcess -ErrorAction SilentlyContinue
            $name = if ($process) { $process.ProcessName } else { "unknown" }
            return "$name PID $($connection.OwningProcess)"
        }
    } catch {
        return $null
    }

    return $null
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

Write-Host "Starting Kafka and Spark for the video flow..." -ForegroundColor DarkCyan
docker compose up -d --build kafka kafka-init spark-processor
if ($LASTEXITCODE -ne 0) {
    throw "Could not start Kafka/Spark. Check Docker Desktop and network, then run this script again."
}

$dashboardReady = $false
try {
    $dashboardResponse = Invoke-WebRequest -Uri "http://localhost:3000/" -UseBasicParsing -TimeoutSec 2
    $dashboardReady = $dashboardResponse.StatusCode -eq 200
} catch {
    $dashboardReady = $false
}

if (-not $dashboardReady) {
    Stop-ProjectDashboardProcesses
    if (-not (Test-Path (Join-Path $projectRoot "node_modules\.bin\vinext.cmd"))) {
        Write-Host "Preparing dashboard dependencies (first run only)..." -ForegroundColor DarkCyan
        npm install
        if ($LASTEXITCODE -ne 0) { throw "Could not install dashboard dependencies." }
    }
    $runtimeDir = Join-Path $projectRoot ".codex_tmp"
    New-Item -ItemType Directory -Force -Path $runtimeDir | Out-Null
    $dashboardLog = Join-Path $runtimeDir "video-dashboard.log"
    $dashboardErrorLog = Join-Path $runtimeDir "video-dashboard-error.log"
    $portOwner = Get-PortOwnerText -Port 3000
    if ($portOwner) {
        throw "Port 3000 is already in use by $portOwner. Close that process, then run this script again."
    }

    Write-Host "Starting video dashboard at http://localhost:3000 ..." -ForegroundColor DarkCyan
    $previousNodeOptions = $env:NODE_OPTIONS
    Remove-Item Env:NODE_OPTIONS -ErrorAction SilentlyContinue
    try {
        $dashboardProcess = Start-Process -FilePath "npm.cmd" -ArgumentList @("run", "dev", "--", "--host", "127.0.0.1", "--port", "3000", "--strictPort") -WorkingDirectory $projectRoot -WindowStyle Hidden -RedirectStandardOutput $dashboardLog -RedirectStandardError $dashboardErrorLog -PassThru
    } finally {
        if ($null -ne $previousNodeOptions) {
            $env:NODE_OPTIONS = $previousNodeOptions
        }
    }
    Set-Content -LiteralPath (Join-Path $runtimeDir "video-dashboard.pid") -Value $dashboardProcess.Id

    for ($attempt = 0; $attempt -lt 60; $attempt++) {
        if ($dashboardProcess.HasExited) {
            $errorTail = ""
            if (Test-Path -LiteralPath $dashboardErrorLog) {
                $errorTail = (Get-Content -LiteralPath $dashboardErrorLog -Tail 20) -join "`n"
            }
            throw "Dashboard process stopped before it became ready. $errorTail"
        }
        try {
            $dashboardResponse = Invoke-WebRequest -Uri "http://localhost:3000/" -UseBasicParsing -TimeoutSec 2
            if ($dashboardResponse.StatusCode -eq 200) {
                $dashboardReady = $true
                break
            }
        } catch {
            if ($attempt -gt 0 -and $attempt % 10 -eq 0) {
                Write-Host "Still waiting for dashboard to compile..." -ForegroundColor DarkGray
            }
            Start-Sleep -Seconds 1
        }
    }
    if (-not $dashboardReady) {
        $errorTail = ""
        if (Test-Path -LiteralPath $dashboardErrorLog) {
            $errorTail = (Get-Content -LiteralPath $dashboardErrorLog -Tail 20) -join "`n"
        }
        throw "Dashboard did not become ready. See .codex_tmp/video-dashboard-error.log. $errorTail"
    }
    Write-Host "Dashboard is ready." -ForegroundColor Green
} else {
    Write-Host "Dashboard is already running at http://localhost:3000" -ForegroundColor Green
}

$venvPython = Join-Path $projectRoot ".venv-video\Scripts\python.exe"
$requirementsFile = Join-Path $projectRoot "services\video_producer\requirements.txt"
$dependencyStamp = Join-Path $projectRoot ".venv-video\requirements.sha256"
$torchCudaStamp = Join-Path $projectRoot ".venv-video\torch-cuda.sha256"
$requirementsHash = (Get-FileHash -Algorithm SHA256 -LiteralPath $requirementsFile).Hash
$torchIndexUrl = if ($env:VIDEO_TORCH_INDEX_URL) { $env:VIDEO_TORCH_INDEX_URL } else { "https://download.pytorch.org/whl/cu130" }
$torchCudaKey = "$requirementsHash|$torchIndexUrl"
$installDependencies = $false
if (-not (Test-Path $venvPython)) {
    Write-Host "Preparing video dependencies (first run only)..." -ForegroundColor DarkCyan
    python -m venv (Join-Path $projectRoot ".venv-video")
    $installDependencies = $true
} elseif (-not (Test-Path $dependencyStamp) -or (Get-Content -LiteralPath $dependencyStamp -Raw).Trim() -ne $requirementsHash) {
    Write-Host "Updating video AI dependencies..." -ForegroundColor DarkCyan
    $installDependencies = $true
}
if ($installDependencies) {
    & $venvPython -m pip install --disable-pip-version-check --prefer-binary -r $requirementsFile
    if ($LASTEXITCODE -ne 0) { throw "Could not install video dependencies." }
    Set-Content -LiteralPath $dependencyStamp -Value $requirementsHash -Encoding ASCII
}

$hasNvidiaGpu = $false
try {
    $nvidiaSmi = Get-Command nvidia-smi -ErrorAction Stop
    & $nvidiaSmi.Source | Out-Null
    $hasNvidiaGpu = $LASTEXITCODE -eq 0
} catch {
    $hasNvidiaGpu = $false
}

if ($hasNvidiaGpu) {
    & $venvPython -c "import torch; raise SystemExit(0 if torch.cuda.is_available() else 1)"
    $torchCudaReady = $LASTEXITCODE -eq 0
    $torchCudaStampReady = (Test-Path $torchCudaStamp) -and (Get-Content -LiteralPath $torchCudaStamp -Raw).Trim() -eq $torchCudaKey
    if (-not $torchCudaReady) {
        Write-Host "Enabling GPU acceleration for video AI..." -ForegroundColor DarkCyan
        & $venvPython -m pip install --disable-pip-version-check --upgrade --force-reinstall torch torchvision --index-url $torchIndexUrl
        if ($LASTEXITCODE -ne 0) { throw "Could not install GPU acceleration packages." }
        & $venvPython -c "import torch; raise SystemExit(0 if torch.cuda.is_available() else 1)"
        if ($LASTEXITCODE -ne 0) { throw "GPU acceleration was installed but CUDA is still unavailable." }
        Set-Content -LiteralPath $torchCudaStamp -Value $torchCudaKey -Encoding ASCII
    } elseif (-not $torchCudaStampReady) {
        Set-Content -LiteralPath $torchCudaStamp -Value $torchCudaKey -Encoding ASCII
    }
}

$videoArgs = @(
    "-m", "services.video_producer.app",
    "--road-name", $RoadName,
    "--latitude", $Latitude.ToString([Globalization.CultureInfo]::InvariantCulture),
    "--longitude", $Longitude.ToString([Globalization.CultureInfo]::InvariantCulture)
)
if ($Source) { $videoArgs += @("--source", $Source) }
if ($Loop) { $videoArgs += "--loop" }
if ($Preview) { $videoArgs += "--preview" }
if ($NoRealtime) { $videoArgs += "--no-realtime" }

Write-Host "Video dashboard: http://localhost:3000" -ForegroundColor Green
Write-Host "Upload a video on the dashboard. Press Ctrl+C to stop the service." -ForegroundColor Yellow
Stop-ProjectVideoApiProcesses
& $venvPython @videoArgs
exit $LASTEXITCODE
