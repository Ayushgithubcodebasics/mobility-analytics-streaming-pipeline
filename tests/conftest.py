"""
conftest.py — Shared pytest fixtures for the traffic pipeline test suite.

The SparkSession here runs in LOCAL mode — no Docker, no Kafka, no Delta Lake.
All DQ functions in silver_transforms.py use only standard PySpark SQL
functions, so they work perfectly in a plain local session.
"""

import os
import sys

import pytest

# Make apps/ importable without installing the package
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "apps"))

# Override env vars so config.py doesn't try to reach Docker services
os.environ.setdefault("SPARK_MASTER_URL", "local[2]")
os.environ.setdefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
os.environ.setdefault("HIVE_METASTORE_URI", "thrift://localhost:9083")
os.environ.setdefault("WAREHOUSE_PATH", "/tmp/test-warehouse")
os.environ.setdefault("SPEED_MIN_KMH", "0")
os.environ.setdefault("SPEED_MAX_KMH", "160")
os.environ.setdefault("LATE_EVENT_MINUTES", "20")
os.environ.setdefault("FUTURE_EVENT_MINUTES", "10")


from pyspark.sql import SparkSession  # noqa: E402


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """
    One SparkSession shared across the whole test session.
    scope=session means it is created once and reused — keeps tests fast.
    ui.enabled=false stops the web UI trying to bind a port during CI.
    """
    return (
        SparkSession.builder.master("local[2]")
        .appName("test-traffic-dq")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.adaptive.enabled", "false")  # deterministic plans
        .getOrCreate()
    )
