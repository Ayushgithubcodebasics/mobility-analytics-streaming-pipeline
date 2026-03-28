param(
    [switch]$SkipTopicCreation
)

$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $projectRoot

Write-Host "[1/4] Starting Docker services..." -ForegroundColor Cyan
docker compose up -d

Write-Host "[2/4] Waiting briefly for services to initialize..." -ForegroundColor Cyan
Start-Sleep -Seconds 10

if (-not $SkipTopicCreation) {
    Write-Host "[3/4] Creating Kafka topic traffic-topic..." -ForegroundColor Cyan
    try {
        docker exec -i kafka /opt/kafka/bin/kafka-topics.sh --create --topic traffic-topic --bootstrap-server kafka:9092 --partitions 3 --replication-factor 1 | Out-Host
    }
    catch {
        Write-Host "Topic may already exist. Continuing..." -ForegroundColor Yellow
    }
}

Write-Host "[4/4] Next commands" -ForegroundColor Green
Write-Host "Producer:" -ForegroundColor Yellow
Write-Host "  python producer\traffic_dirty_producer.py --bootstrap localhost:29092"
Write-Host "Bronze:" -ForegroundColor Yellow
Write-Host "  docker exec -it spark-worker /opt/spark/bin/spark-submit --conf spark.jars.ivy=/tmp/.ivy --packages io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 /opt/spark-apps/traffic_bronze.py"
Write-Host "Silver:" -ForegroundColor Yellow
Write-Host "  docker exec -it spark-worker /opt/spark/bin/spark-submit --conf spark.jars.ivy=/tmp/.ivy --packages io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 /opt/spark-apps/traffic_silver.py"
Write-Host "Gold:" -ForegroundColor Yellow
Write-Host "  docker exec -it spark-worker /opt/spark/bin/spark-submit --conf spark.jars.ivy=/tmp/.ivy --packages io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 /opt/spark-apps/traffic_gold.py"
Write-Host "Spark SQL registration:" -ForegroundColor Yellow
Write-Host "  docker exec -it spark-worker /opt/spark/bin/spark-sql --packages io.delta:delta-spark_2.12:3.2.0 --conf spark.jars.ivy=/tmp/.ivy --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog --conf spark.sql.catalogImplementation=hive --conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083 --conf spark.sql.warehouse.dir=/opt/spark/warehouse -f /opt/spark-scripts/register_views.sql"
