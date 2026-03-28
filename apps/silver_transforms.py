"""
silver_transforms.py  —  Pure DQ transformation functions for the Silver layer
═══════════════════════════════════════════════════════════════════════════════
All functions here take a DataFrame and return a DataFrame.
They have NO side effects, NO Spark session references, NO file I/O.

This design has two benefits:
  1. TESTABILITY  — pytest can import and test each function in local Spark
                    mode without Docker, Kafka or Delta Lake.
  2. REUSABILITY  — traffic_silver.py calls them inside foreachBatch;
                    the same functions power all unit tests.

DQ Pipeline order:
  apply_dq_flags          → flag corrupt / null records
  apply_type_casting      → safe casts (speed str→int, event_time str→ts)
  apply_business_validation → range/window checks
  filter_clean_records    → drop anything not flagged OK + valid
  apply_feature_engineering → enrich surviving records
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    current_timestamp,
    expr,
    hour,
    to_timestamp,
    when,
)


def apply_dq_flags(df: DataFrame) -> DataFrame:
    """
    Step 1 — Attach a dq_flag column to every row.

    Priority order matters: CORRUPT_JSON is checked first because a
    corrupted payload will also have null vehicle_id / event_time, and
    we want the most specific flag, not a generic MISSING_VEHICLE.
    """
    return df.withColumn(
        "dq_flag",
        when(col("raw_json").contains("CORRUPTED"), "CORRUPT_JSON")
        .when(col("vehicle_id").isNull(), "MISSING_VEHICLE")
        .when(col("event_time").isNull(), "MISSING_TIME")
        .when(col("congestion_level").isNull(), "MISSING_CONGESTION")
        .otherwise("OK"),
    )


def apply_type_casting(df: DataFrame) -> DataFrame:
    """
    Step 2 — Safe type promotion.

    speed_int: non-numeric strings (FAST, SLOW, UNKNOWN) silently become
               null after cast, which apply_business_validation then rejects.
    event_ts:  ISO-8601 strings → TIMESTAMP; malformed strings → null → rejected.
    """
    return df.withColumn("speed_int", col("speed").cast("int")).withColumn(
        "event_ts", to_timestamp("event_time")
    )


def apply_business_validation(df: DataFrame) -> DataFrame:
    """
    Step 3 — Domain rule checks, stored as 0/1 flag columns.

    speed_valid rules (all must pass):
      ✗ null speed    (null_speed dirty type)     → cast to null → 0
      ✗ negative      (negative_speed dirty type) → < 0          → 0
      ✗ extreme       (extreme_speed dirty type)  → > 160 km/h   → 0
      ✗ string speed  (wrong_datatype dirty type) → cast to null → 0

    time_valid rules (both must pass):
      ✗ too old:    older than LATE_EVENT_MINUTES  (late_event dirty type)
      ✗ too future: more than FUTURE_EVENT_MINUTES (future_event dirty type)

    Thresholds are read from config so they can be overridden via env vars
    without touching source code.
    """
    from config import cfg  # imported here to keep module testable without cfg side-effects

    return df.withColumn(
        "speed_valid",
        when(
            (col("speed_int") >= cfg.SPEED_MIN_KMH) & (col("speed_int") <= cfg.SPEED_MAX_KMH),
            1,
        ).otherwise(0),
    ).withColumn(
        "time_valid",
        when(
            (
                col("event_ts")
                >= current_timestamp() - expr(f"INTERVAL {cfg.LATE_EVENT_MINUTES} MINUTES")
            )
            & (
                col("event_ts")
                <= current_timestamp() + expr(f"INTERVAL {cfg.FUTURE_EVENT_MINUTES} MINUTES")
            ),
            1,
        ).otherwise(0),
    )


def filter_clean_records(df: DataFrame) -> DataFrame:
    """
    Step 4 — Keep only records that pass every DQ check.

    A record is clean if and only if:
      - dq_flag  == "OK"     (no structural / null issues)
      - speed_valid == 1     (speed in valid range)
      - time_valid  == 1     (timestamp in valid window)
    """
    return df.filter(
        (col("dq_flag") == "OK") & (col("speed_valid") == 1) & (col("time_valid") == 1)
    )


def apply_feature_engineering(df: DataFrame) -> DataFrame:
    """
    Step 5 — Enrich clean records with derived columns.

    hour      : integer hour extracted from event_ts (for hourly charts)
    peak_flag : 1 if event falls in morning (08-11) or evening (17-20) rush hour
    speed_band: categorical label for speed range (LOW / MEDIUM / HIGH)
    """
    return (
        df.withColumn("hour", hour("event_ts"))
        .withColumn(
            "peak_flag",
            when(
                col("hour").between(8, 11) | col("hour").between(17, 20),
                1,
            ).otherwise(0),
        )
        .withColumn(
            "speed_band",
            when(col("speed_int") < 30, "LOW")
            .when(col("speed_int") < 70, "MEDIUM")
            .otherwise("HIGH"),
        )
    )
