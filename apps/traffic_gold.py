"""
traffic_gold.py  —  Gold Layer  (Star Schema + Business-Ready Tables)
══════════════════════════════════════════════════════════════════════
Reads the silver Delta stream and produces three Gold tables:
  - dim_zone      : city zone dimension
  - dim_road      : road type dimension
  - fact_traffic  : merge-based fact table for idempotent loads
"""

import logging
import os
import sys
import time

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, to_date, when

sys.path.insert(0, os.path.dirname(__file__))

from config import cfg

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("gold")


log.info("Starting Gold | master=%s", cfg.SPARK_MASTER)

spark = (
    SparkSession.builder.appName("TrafficGold")
    .master(cfg.SPARK_MASTER)
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .config("spark.sql.shuffle.partitions", cfg.SHUFFLE_PARTITIONS)
    .config("spark.hadoop.hive.metastore.uris", cfg.HIVE_METASTORE_URI)
    .enableHiveSupport()
    .getOrCreate()
)
spark.sparkContext.setLogLevel(cfg.LOG_LEVEL)


def upsert_dim(dim_df: DataFrame, path: str, natural_key: str) -> int:
    if dim_df.rdd.isEmpty():
        return 0

    row_count = dim_df.count()

    if DeltaTable.isDeltaTable(spark, path):
        log.info("  MERGE into %s on %s (%d incoming rows)", path, natural_key, row_count)
        (
            DeltaTable.forPath(spark, path)
            .alias("target")
            .merge(dim_df.alias("source"), f"target.{natural_key} = source.{natural_key}")
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        log.info("  CREATE (first batch) %s (%d rows)", path, row_count)
        dim_df.write.format("delta").mode("overwrite").save(path)

    return row_count


def upsert_fact(fact_df: DataFrame, path: str) -> int:
    if fact_df.rdd.isEmpty():
        return 0

    row_count = fact_df.count()
    merge_condition = " AND ".join(
        [
            "target.vehicle_id = source.vehicle_id",
            "target.road_id = source.road_id",
            "target.city_zone = source.city_zone",
            "target.event_ts = source.event_ts",
        ]
    )

    if DeltaTable.isDeltaTable(spark, path):
        log.info("  MERGE into %s (%d incoming fact rows)", path, row_count)
        (
            DeltaTable.forPath(spark, path)
            .alias("target")
            .merge(fact_df.alias("source"), merge_condition)
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        log.info("  CREATE (first batch) %s (%d fact rows)", path, row_count)
        fact_df.write.format("delta").mode("overwrite").save(path)

    return row_count


def process_gold_batch(batch_df: DataFrame, epoch_id: int) -> None:
    total_in = batch_df.count()

    if total_in == 0:
        log.info("Gold batch=%d | empty, skipping", epoch_id)
        return

    log.info("Gold batch=%d | input_rows=%d", epoch_id, total_in)
    t_start = time.monotonic()
    batch_df.cache()

    try:
        dim_zone_df = (
            batch_df.select("city_zone")
            .dropDuplicates(["city_zone"])
            .withColumn(
                "zone_type",
                when(col("city_zone") == "CBD", "Commercial")
                .when(col("city_zone") == "TECHPARK", "IT Hub")
                .when(col("city_zone").isin("AIRPORT", "TRAINSTATION"), "Transit Hub")
                .otherwise("Residential"),
            )
            .withColumn(
                "traffic_risk",
                when(col("city_zone").isin("CBD", "AIRPORT", "TRAINSTATION"), "HIGH")
                .when(col("city_zone") == "TECHPARK", "MEDIUM")
                .otherwise("LOW"),
            )
        )
        zone_rows = upsert_dim(dim_zone_df, cfg.DIM_ZONE_PATH, "city_zone")

        dim_road_df = (
            batch_df.select("road_id")
            .dropDuplicates(["road_id"])
            .withColumn(
                "road_type",
                when(col("road_id").isin("R100", "R200"), "Highway").otherwise("City Road"),
            )
            .withColumn(
                "speed_limit",
                when(col("road_id").isin("R100", "R200"), 100).otherwise(60),
            )
        )
        road_rows = upsert_dim(dim_road_df, cfg.DIM_ROAD_PATH, "road_id")

        fact_df = (
            batch_df.select(
                "vehicle_id",
                "road_id",
                "city_zone",
                col("speed_int").alias("speed"),
                "congestion_level",
                "event_ts",
                "peak_flag",
                "speed_band",
                "hour",
                "weather",
            )
            .withColumn("event_date", to_date("event_ts"))
            .withColumn("ingested_at", current_timestamp())
        )
        fact_rows = upsert_fact(fact_df, cfg.FACT_PATH)

        elapsed_ms = int((time.monotonic() - t_start) * 1000)
        log.info(
            "Gold batch=%d | dim_zone_rows=%d | dim_road_rows=%d | "
            "fact_rows_written=%d | elapsed_ms=%d",
            epoch_id,
            zone_rows,
            road_rows,
            fact_rows,
            elapsed_ms,
        )

    except Exception:
        log.exception("Gold batch=%d | FAILED", epoch_id)
        raise
    finally:
        batch_df.unpersist()


log.info("Reading silver Delta stream from %s", cfg.SILVER_PATH)

silver_stream = (
    spark.readStream.format("delta").option("ignoreChanges", "true").load(cfg.SILVER_PATH)
)

gold_query = (
    silver_stream.writeStream.foreachBatch(process_gold_batch)
    .option("checkpointLocation", cfg.GOLD_CHK)
    .trigger(processingTime=cfg.GOLD_TRIGGER)
    .start()
)

spark.streams.awaitAnyTermination()
