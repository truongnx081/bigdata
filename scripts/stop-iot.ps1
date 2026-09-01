$ErrorActionPreference = "Stop"
docker compose stop iot-producer
Write-Host "IoT source on port 8101 stopped. Kafka and Spark are still running." -ForegroundColor Yellow
