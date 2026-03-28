"""
traffic_silver.py  —  Silver Layer  (Data Quality + Cleaning)
══════════════════════════════════════════════════════════════════════
Reads the bronze Delta stream, applies data quality checks, filters
and quarantines bad records, deduplicates within each batch, and
writes clean Silver records for downstream Gold processing.
"""

import logging
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, current_timestamp, lit, when
from pyspark.sql.window import Window

sys.path.insert(0, os.path.dirname(__file__))

from config import cfg
from silver_transforms import (
    apply_business_validation,
    apply_dq_flags,
    apply_feature_engineering,
    apply_type_casting,
    filter_clean_records,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("silver")


log.info("Starting Silver | master=%s", cfg.SPARK_MASTER)

spark = (
    SparkSession.builder.appName("TrafficSilver")
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


def _label_reject_reason(df):
    return df.withColumn(
        "reject_reason",
        when(col("dq_flag") != "OK", col("dq_flag"))
        .when(col("speed_valid") == 0, lit("INVALID_SPEED"))
        .when(col("time_valid") == 0, lit("INVALID_TIME"))
        .otherwise(lit("OK")),
    )


def process_silver_batch(batch_df, epoch_id: int) -> None:
    total_in = batch_df.count()

    if total_in == 0:
        log.info("Silver batch=%d | empty batch, skipping", epoch_id)
        return

    batch_df.cache()
    try:
        dq_df = apply_dq_flags(batch_df)
        typed = apply_type_casting(dq_df)
        validated = apply_business_validation(typed)
        labeled = _label_reject_reason(validated)

        rejected_pre_dedup = labeled.filter(col("reject_reason") != "OK").withColumn(
            "rejected_at", current_timestamp()
        )

        clean = filter_clean_records(validated)

        dup_window = Window.partitionBy("vehicle_id", "event_ts")
        duplicate_checked = clean.withColumn("duplicate_count", count("*").over(dup_window))
        duplicates = (
            duplicate_checked.filter(col("duplicate_count") > 1)
            .withColumn("reject_reason", lit("DUPLICATE_EVENT"))
            .withColumn("rejected_at", current_timestamp())
        )
        deduped = duplicate_checked.filter(col("duplicate_count") == 1).drop("duplicate_count")
        final = apply_feature_engineering(deduped)

        dq_breakdown = {
            r["dq_flag"]: r["count"]
            for r in dq_df.groupBy("dq_flag").count().collect()
        }
        speed_rejects = int(validated.filter(col("speed_valid") == 0).count())
        time_rejects = int(validated.filter(col("time_valid") == 0).count())
        duplicate_rejects = int(duplicates.count())
        quarantine_rows = int(rejected_pre_dedup.count()) + duplicate_rejects
        total_out = final.count()
        rejected = total_in - total_out
        reject_rate = round(100.0 * rejected / total_in, 1) if total_in else 0.0

        log.info(
            "Silver batch=%d | in=%d | out=%d | rejected=%d (%.1f%%) | quarantine=%d",
            epoch_id,
            total_in,
            total_out,
            rejected,
            reject_rate,
            quarantine_rows,
        )
        log.info(
            "Silver batch=%d | dq_flags=%s | speed_rejects=%d | "
            "time_rejects=%d | duplicate_rejects=%d",
            epoch_id,
            dq_breakdown,
            speed_rejects,
            time_rejects,
            duplicate_rejects,
        )

        if quarantine_rows:
            rejected_union = rejected_pre_dedup.select(*rejected_pre_dedup.columns)
            if duplicate_rejects:
                duplicate_cols = rejected_union.columns
                duplicates_to_write = duplicates
                for missing_col in [
                    c for c in duplicate_cols if c not in duplicates_to_write.columns
                ]:
                    duplicates_to_write = duplicates_to_write.withColumn(missing_col, lit(None))
                duplicates_to_write = duplicates_to_write.select(*duplicate_cols)
                rejected_union = rejected_union.unionByName(duplicates_to_write)

            (
                rejected_union.write.format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .save(cfg.REJECTS_PATH)
            )

        (
            final.write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .save(cfg.SILVER_PATH)
        )
        log.info("Silver batch=%d | wrote %d rows → %s", epoch_id, total_out, cfg.SILVER_PATH)

    except Exception:
        log.exception("Silver batch=%d | FAILED — batch dropped", epoch_id)
        raise
    finally:
        batch_df.unpersist()


log.info("Reading bronze Delta stream from %s", cfg.BRONZE_PATH)

bronze_stream = (
    spark.readStream.format("delta").option("ignoreChanges", "true").load(cfg.BRONZE_PATH)
)

silver_query = (
    bronze_stream.writeStream.foreachBatch(process_silver_batch)
    .option("checkpointLocation", cfg.SILVER_CHK)
    .trigger(processingTime=cfg.SILVER_TRIGGER)
    .start()
)

spark.streams.awaitAnyTermination()
