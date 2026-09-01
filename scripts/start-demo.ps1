$ErrorActionPreference = "Stop"
Write-Host "Starting the IoT realtime traffic demo..." -ForegroundColor Green
Write-Host "Open http://localhost:8101 when Kafka and Spark are ready." -ForegroundColor DarkCyan
docker compose up --build
if ($LASTEXITCODE -ne 0) {
    throw "Could not start demo. Check Docker Desktop and network, then run this script again."
}
