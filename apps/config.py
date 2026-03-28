"""
config.py  —  Centralised runtime configuration
═══════════════════════════════════════════════════════════════════
Every value that was previously hardcoded inside the Spark apps is
read from environment variables here. Docker Compose injects them
automatically via the .env file.
"""

import os


class _Config:
    """Singleton-style config bag. All values resolved at import time."""

    SPARK_MASTER: str = os.environ.get("SPARK_MASTER_URL", "spark://spark-master:7077")
    SHUFFLE_PARTITIONS: str = os.environ.get("SHUFFLE_PARTITIONS", "4")
    LOG_LEVEL: str = os.environ.get("SPARK_LOG_LEVEL", "WARN")

    KAFKA_BOOTSTRAP_SERVERS: str = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    KAFKA_TOPIC: str = os.environ.get("KAFKA_TOPIC", "traffic-topic")
    MAX_OFFSETS_PER_TRIGGER: int = int(os.environ.get("MAX_OFFSETS_PER_TRIGGER", "2000"))

    HIVE_METASTORE_URI: str = os.environ.get("HIVE_METASTORE_URI", "thrift://hive-metastore:9083")

    WAREHOUSE_PATH: str = os.environ.get("WAREHOUSE_PATH", "/opt/spark/warehouse")

    @property
    def BRONZE_PATH(self) -> str:
        return f"{self.WAREHOUSE_PATH}/traffic_bronze"

    @property
    def SILVER_PATH(self) -> str:
        return f"{self.WAREHOUSE_PATH}/traffic_silver"

    @property
    def REJECTS_PATH(self) -> str:
        return f"{self.WAREHOUSE_PATH}/traffic_rejects"

    @property
    def DIM_ZONE_PATH(self) -> str:
        return f"{self.WAREHOUSE_PATH}/dim_zone"

    @property
    def DIM_ROAD_PATH(self) -> str:
        return f"{self.WAREHOUSE_PATH}/dim_road"

    @property
    def FACT_PATH(self) -> str:
        return f"{self.WAREHOUSE_PATH}/fact_traffic"

    @property
    def BRONZE_CHK(self) -> str:
        return f"{self.WAREHOUSE_PATH}/chk/traffic_bronze"

    @property
    def SILVER_CHK(self) -> str:
        return f"{self.WAREHOUSE_PATH}/chk/traffic_silver"

    @property
    def GOLD_CHK(self) -> str:
        return f"{self.WAREHOUSE_PATH}/chk/gold_unified"

    BRONZE_TRIGGER: str = os.environ.get("BRONZE_TRIGGER_INTERVAL", "10 seconds")
    SILVER_TRIGGER: str = os.environ.get("SILVER_TRIGGER_INTERVAL", "15 seconds")
    GOLD_TRIGGER: str = os.environ.get("GOLD_TRIGGER_INTERVAL", "30 seconds")

    SPEED_MIN_KMH: int = int(os.environ.get("SPEED_MIN_KMH", "0"))
    SPEED_MAX_KMH: int = int(os.environ.get("SPEED_MAX_KMH", "160"))
    LATE_EVENT_MINUTES: int = int(os.environ.get("LATE_EVENT_MINUTES", "20"))
    FUTURE_EVENT_MINUTES: int = int(os.environ.get("FUTURE_EVENT_MINUTES", "10"))
    WATERMARK_MINUTES: int = int(os.environ.get("WATERMARK_MINUTES", "15"))


cfg = _Config()
