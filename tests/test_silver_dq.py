"""
test_silver_dq.py — Unit tests for the Silver layer DQ pipeline.

Each test:
  1. Builds a minimal one-row DataFrame with a specific fault
  2. Runs it through the relevant pure function from silver_transforms.py
  3. Asserts the expected output column value

No Docker, no Kafka, no Delta Lake required.
Run with:  pytest tests/ -v
"""

from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from silver_transforms import (
    apply_business_validation,
    apply_dq_flags,
    apply_feature_engineering,
    apply_type_casting,
    filter_clean_records,
)

BRONZE_TEST_SCHEMA = StructType(
    [
        StructField("raw_json", StringType(), True),
        StructField("vehicle_id", StringType(), True),
        StructField("road_id", StringType(), True),
        StructField("city_zone", StringType(), True),
        StructField("speed", StringType(), True),
        StructField("congestion_level", IntegerType(), True),
        StructField("weather", StringType(), True),
        StructField("event_time", StringType(), True),
    ]
)


# ── Helpers ───────────────────────────────────────────────────────────


def _now_iso(offset_minutes: int = 0) -> str:
    """Return an ISO-8601 UTC timestamp offset by *offset_minutes*."""
    ts = datetime.now(timezone.utc) + timedelta(minutes=offset_minutes)
    return ts.isoformat()


def _make_row(spark: SparkSession, **overrides):
    """
    Build a single-row DataFrame that looks like a bronze record.
    Default values form a perfectly valid record; callers override one
    field at a time to inject a specific fault.
    """
    defaults = {
        "raw_json": '{"vehicle_id":"abc","speed":"60"}',
        "vehicle_id": "test-vehicle-001",
        "road_id": "R100",
        "city_zone": "CBD",
        "speed": "60",  # string — silver casts to int
        "congestion_level": 3,
        "weather": "CLEAR",
        "event_time": _now_iso(0),  # right now → always valid
    }
    defaults.update(overrides)
    return spark.createDataFrame([defaults], schema=BRONZE_TEST_SCHEMA)


# ══════════════════════════════════════════════════════════════════════
# GROUP 1: apply_dq_flags
# ══════════════════════════════════════════════════════════════════════


class TestDQFlags:
    def test_clean_record_flagged_ok(self, spark):
        """A fully valid record must receive dq_flag = 'OK'."""
        df = _make_row(spark)
        result = apply_dq_flags(df).collect()[0]
        assert result["dq_flag"] == "OK"

    def test_corrupt_json_detected(self, spark):
        """raw_json containing 'CORRUPTED' must be flagged CORRUPT_JSON."""
        df = _make_row(spark, raw_json='{"raw":"###CORRUPTED_EVENT###","corrupted":true}')
        result = apply_dq_flags(df).collect()[0]
        assert result["dq_flag"] == "CORRUPT_JSON"

    def test_missing_vehicle_id_flagged(self, spark):
        """Null vehicle_id must produce MISSING_VEHICLE flag."""
        df = _make_row(spark, vehicle_id=None)
        result = apply_dq_flags(df).collect()[0]
        assert result["dq_flag"] == "MISSING_VEHICLE"

    def test_missing_event_time_flagged(self, spark):
        """Null event_time must produce MISSING_TIME flag."""
        df = _make_row(spark, event_time=None)
        result = apply_dq_flags(df).collect()[0]
        assert result["dq_flag"] == "MISSING_TIME"

    def test_corrupt_json_takes_priority_over_null_vehicle(self, spark):
        """
        A corrupted payload will also have null vehicle_id after parsing,
        but CORRUPT_JSON must win because it is checked first.
        """
        df = _make_row(
            spark,
            raw_json='{"raw":"###CORRUPTED_EVENT###"}',
            vehicle_id=None,
        )
        result = apply_dq_flags(df).collect()[0]
        assert result["dq_flag"] == "CORRUPT_JSON"


# ══════════════════════════════════════════════════════════════════════
# GROUP 2: apply_type_casting
# ══════════════════════════════════════════════════════════════════════


class TestTypeCasting:
    def test_numeric_speed_string_casts_to_int(self, spark):
        """speed='60' must cast to speed_int=60."""
        df = apply_type_casting(_make_row(spark, speed="60"))
        assert df.collect()[0]["speed_int"] == 60

    def test_string_speed_casts_to_null(self, spark):
        """
        speed='FAST' (wrong datatype dirty type) must cast to null.
        Null speed_int is then rejected by apply_business_validation.
        """
        df = apply_type_casting(_make_row(spark, speed="FAST"))
        assert df.collect()[0]["speed_int"] is None

    def test_none_speed_stays_null(self, spark):
        """speed=None (null_speed dirty type) must stay null after cast."""
        df = apply_type_casting(_make_row(spark, speed=None))
        assert df.collect()[0]["speed_int"] is None

    def test_valid_iso_timestamp_parsed(self, spark):
        """A well-formed ISO-8601 string must produce a non-null event_ts."""
        df = apply_type_casting(_make_row(spark, event_time=_now_iso()))
        assert df.collect()[0]["event_ts"] is not None


# ══════════════════════════════════════════════════════════════════════
# GROUP 3: apply_business_validation  (speed_valid and time_valid)
# ══════════════════════════════════════════════════════════════════════


def _validated_row(spark, **overrides):
    """Convenience: apply full type casting + validation pipeline."""
    df = _make_row(spark, **overrides)
    return apply_business_validation(apply_type_casting(apply_dq_flags(df))).collect()[0]


class TestSpeedValidation:
    def test_valid_speed_passes(self, spark):
        assert _validated_row(spark, speed="80")["speed_valid"] == 1

    def test_zero_speed_passes(self, spark):
        """Stationary vehicle (speed=0) is valid."""
        assert _validated_row(spark, speed="0")["speed_valid"] == 1

    def test_boundary_max_speed_passes(self, spark):
        """160 km/h is the exact upper boundary — must pass."""
        assert _validated_row(spark, speed="160")["speed_valid"] == 1

    def test_negative_speed_rejected(self, spark):
        """negative_speed dirty type: speed='-40' must fail."""
        assert _validated_row(spark, speed="-40")["speed_valid"] == 0

    def test_extreme_speed_rejected(self, spark):
        """extreme_speed dirty type: speed='420' must fail."""
        assert _validated_row(spark, speed="420")["speed_valid"] == 0

    def test_string_speed_rejected(self, spark):
        """wrong_datatype dirty type: speed='FAST' → null → must fail."""
        assert _validated_row(spark, speed="FAST")["speed_valid"] == 0

    def test_null_speed_rejected(self, spark):
        """null_speed dirty type: speed=None → null → must fail."""
        assert _validated_row(spark, speed=None)["speed_valid"] == 0


class TestTimeValidation:
    def test_current_timestamp_passes(self, spark):
        """An event timestamped right now must be valid."""
        assert _validated_row(spark, event_time=_now_iso(0))["time_valid"] == 1

    def test_late_event_rejected(self, spark):
        """late_event dirty type: 90 minutes in the past must fail."""
        assert _validated_row(spark, event_time=_now_iso(-90))["time_valid"] == 0

    def test_future_event_rejected(self, spark):
        """future_event dirty type: 60 minutes in the future must fail."""
        assert _validated_row(spark, event_time=_now_iso(+60))["time_valid"] == 0

    def test_slightly_old_event_passes(self, spark):
        """5 minutes old is within the 20-minute late window — must pass."""
        assert _validated_row(spark, event_time=_now_iso(-5))["time_valid"] == 1


# ══════════════════════════════════════════════════════════════════════
# GROUP 4: filter_clean_records
# ══════════════════════════════════════════════════════════════════════


class TestFilterCleanRecords:
    def test_fully_valid_record_survives(self, spark):
        df = apply_business_validation(apply_type_casting(apply_dq_flags(_make_row(spark))))
        assert filter_clean_records(df).count() == 1

    def test_corrupt_json_filtered_out(self, spark):
        df = apply_business_validation(
            apply_type_casting(
                apply_dq_flags(_make_row(spark, raw_json='{"raw":"###CORRUPTED_EVENT###"}'))
            )
        )
        assert filter_clean_records(df).count() == 0

    def test_invalid_speed_filtered_out(self, spark):
        df = apply_business_validation(
            apply_type_casting(apply_dq_flags(_make_row(spark, speed="-10")))
        )
        assert filter_clean_records(df).count() == 0


# ══════════════════════════════════════════════════════════════════════
# GROUP 5: apply_feature_engineering
# ══════════════════════════════════════════════════════════════════════


def _engineered_row(spark, speed_int: int, hour_override: int = 14):
    """
    Build a row as it looks after DQ filtering, then apply feature engineering.
    We inject hour directly because event_ts hour depends on wall-clock time.
    """
    from pyspark.sql.functions import lit, when

    df = apply_type_casting(_make_row(spark, speed=str(speed_int)))
    df = apply_feature_engineering(df).withColumn("hour", lit(hour_override))
    df = df.withColumn(
        "peak_flag",
        when(col("hour").between(8, 11) | col("hour").between(17, 20), 1).otherwise(0),
    )
    return df.collect()[0]


class TestFeatureEngineering:
    def test_low_speed_band(self, spark):
        assert _engineered_row(spark, speed_int=20)["speed_band"] == "LOW"

    def test_medium_speed_band(self, spark):
        assert _engineered_row(spark, speed_int=50)["speed_band"] == "MEDIUM"

    def test_high_speed_band(self, spark):
        assert _engineered_row(spark, speed_int=100)["speed_band"] == "HIGH"

    def test_boundary_low_medium(self, spark):
        """speed=30 is the boundary: LOW is < 30, so 30 must be MEDIUM."""
        assert _engineered_row(spark, speed_int=30)["speed_band"] == "MEDIUM"

    def test_morning_peak_flag(self, spark):
        """Hour 9 (morning rush) must set peak_flag=1."""
        row = _engineered_row(spark, speed_int=60, hour_override=9)
        assert row["peak_flag"] == 1

    def test_evening_peak_flag(self, spark):
        """Hour 18 (evening rush) must set peak_flag=1."""
        row = _engineered_row(spark, speed_int=60, hour_override=18)
        assert row["peak_flag"] == 1

    def test_off_peak_flag(self, spark):
        """Hour 14 is outside rush windows, so peak_flag=0."""
        row = _engineered_row(spark, speed_int=60, hour_override=14)
        assert row["peak_flag"] == 0
