"""
traffic_bronze.py  —  Bronze Layer  (Raw Ingestion from Kafka)
══════════════════════════════════════════════════════════════════════
Reads the raw Kafka stream, assigns a flexible schema, and writes
to the Delta Lake bronze table with NO cleaning applied.

Changes from v1:
  - All hardcoded values (master URL, Kafka servers, warehouse path, etc.)
    replaced with os.environ.get() via apps/config.py
  - StreamingQueryListener added: logs batch_id, input_rows,
    output_rows and processing time for every micro-batch
  - Log level controlled by SPARK_LOG_LEVEL env var

Submit (inside spark-worker container):
    /opt/spark/bin/spark-submit \
      --conf spark.jars.ivy=/tmp/.ivy \
      --packages io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
      /opt/spark-apps/traffic_bronze.py
"""

import logging
import os
import sys

# make sibling modules importable when submitted via spark-submit
sys.path.insert(0, os.path.dirname(__file__))

from config import cfg  # noqa: E402
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.streaming import StreamingQueryListener
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# ── Python logger (separate from Spark log4j) ─────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("bronze")

# ── Spark Session ─────────────────────────────────────────────────────
log.info("Starting Bronze | master=%s | topic=%s", cfg.SPARK_MASTER, cfg.KAFKA_TOPIC)

spark = (
    SparkSession.builder.appName("TrafficBronze")
    .master(cfg.SPARK_MASTER)
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.shuffle.partitions", cfg.SHUFFLE_PARTITIONS)
    .config("spark.hadoop.hive.metastore.uris", cfg.HIVE_METASTORE_URI)
    .enableHiveSupport()
    .getOrCreate()
)
spark.sparkContext.setLogLevel(cfg.LOG_LEVEL)


# ── Per-batch progress listener ───────────────────────────────────────
class BronzeProgressListener(StreamingQueryListener):
    """Logs key metrics after every completed micro-batch."""

    def onQueryStarted(self, event):
        log.info("Bronze streaming query started | id=%s", event.id)

    def onQueryProgress(self, event):
        p = event.progress
        sink_rows = p.sink.numOutputRows if p.sink else 0
        log.info(
            "Bronze batch=%d | input_rows=%d | written_rows=%d | "
            "processing_ms=%d | rows_per_sec=%.1f",
            p.batchId,
            p.numInputRows,
            sink_rows,
            p.durationMs.get("triggerExecution", 0),
            p.processedRowsPerSecond or 0.0,
        )

    def onQueryTerminated(self, event):
        log.info("Bronze streaming query terminated | id=%s", event.id)


spark.streams.addListener(BronzeProgressListener())

# ── Kafka Raw Stream ──────────────────────────────────────────────────
log.info(
    "Connecting to Kafka | servers=%s | topic=%s | maxOffsets=%d",
    cfg.KAFKA_BOOTSTRAP_SERVERS,
    cfg.KAFKA_TOPIC,
    cfg.MAX_OFFSETS_PER_TRIGGER,
)

raw_stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", cfg.KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", cfg.KAFKA_TOPIC)
    # latest + checkpointing: on restart, picks up where it left off
    .option("startingOffsets", "latest")
    # cap records per micro-batch to prevent memory pressure
    .option("maxOffsetsPerTrigger", cfg.MAX_OFFSETS_PER_TRIGGER)
    # tolerate Kafka log compaction / segment expiry without crashing
    .option("failOnDataLoss", "false")
    # NOTE: Do NOT set kafka.group.id — Structured Streaming manages
    # Kafka offsets via checkpoints internally.  Setting group.id causes
    # consumer group conflicts that silently prevent ingestion.
    .load()
)

# ── Decode bytes → JSON string ────────────────────────────────────────
json_stream = raw_stream.selectExpr(
    "CAST(value AS STRING) AS raw_json",
    "timestamp AS kafka_timestamp",
    "partition AS kafka_partition",
    "offset    AS kafka_offset",
)

# ── Schema ────────────────────────────────────────────────────────────
# speed stays StringType: dirty events inject "FAST"/"SLOW" strings here.
# event_time stays StringType: no native DateTimeType in SparkSQL;
# both are cast to their proper types in silver after DQ validation.
traffic_schema = StructType(
    [
        StructField("vehicle_id", StringType(), True),
        StructField("road_id", StringType(), True),
        StructField("city_zone", StringType(), True),
        StructField("speed", StringType(), True),
        StructField("congestion_level", IntegerType(), True),
        StructField("weather", StringType(), True),
        StructField("event_time", StringType(), True),
    ]
)

parsed = json_stream.withColumn("data", from_json(col("raw_json"), traffic_schema))
flattened = parsed.select(
    "raw_json",
    "kafka_timestamp",
    "kafka_partition",
    "kafka_offset",
    "data.*",
)

# ── Bronze Delta Write ────────────────────────────────────────────────
log.info("Writing bronze Delta table → %s", cfg.BRONZE_PATH)

bronze_query = (
    flattened.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", cfg.BRONZE_CHK)
    .option("path", cfg.BRONZE_PATH)
    .option("mergeSchema", "true")
    .trigger(processingTime=cfg.BRONZE_TRIGGER)
    .start()
)

spark.streams.awaitAnyTermination()
