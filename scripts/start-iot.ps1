$ErrorActionPreference = "Stop"
Write-Host "Starting Kafka and Spark for the independent IoT flow..." -ForegroundColor DarkCyan
docker compose up -d --build kafka kafka-init spark-processor
if ($LASTEXITCODE -ne 0) {
    throw "Could not start Kafka/Spark. Check Docker Desktop and network, then run this script again."
}
Write-Host "IoT web and alerts: http://localhost:8101 (Ctrl+C stops only this source)" -ForegroundColor Green
docker compose up --build --no-deps iot-producer
if ($LASTEXITCODE -ne 0) {
    throw "Could not start IoT source."
}
