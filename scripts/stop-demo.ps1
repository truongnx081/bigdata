$ErrorActionPreference = "Stop"
docker compose down
Write-Host "Demo stopped. Kafka and Spark data volumes are preserved." -ForegroundColor Yellow
