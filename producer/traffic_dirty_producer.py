"""
traffic_dirty_producer.py
Real-Time Traffic Data Producer - Kafka - Portfolio-Ready
"""

import argparse
import json
import logging
import os
import random
import signal
import time
from collections import deque
from copy import deepcopy
from datetime import datetime, timedelta

import pytz
from faker import Faker
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("producer")

DEFAULT_BOOTSTRAP = os.environ.get("PRODUCER_BOOTSTRAP_SERVERS", "localhost:29092")
DEFAULT_TOPIC = os.environ.get("KAFKA_TOPIC", "traffic-topic")
EVENT_CACHE_MAX = int(os.environ.get("PRODUCER_EVENT_CACHE_MAX", "500"))

ROADS = ["R100", "R200", "R300", "R400"]
ZONES = ["CBD", "AIRPORT", "TECHPARK", "SUBURB", "TRAINSTATION"]
WEATHER = ["CLEAR", "RAIN", "FOG", "STORM"]
DIRTY_TYPES = [
    "null_speed",
    "negative_speed",
    "extreme_speed",
    "duplicate_event",
    "late_event",
    "future_event",
    "wrong_datatype",
    "schema_drift",
    "corrupt_json",
]

fake = Faker()
recent_events = deque(maxlen=EVENT_CACHE_MAX)
producer = None
shutdown_flag = False
sent_count = 0
error_count = 0


def _handle_shutdown(signum, frame):
    del signum, frame
    global shutdown_flag
    log.info("Shutdown signal received - flushing producer...")
    shutdown_flag = True


signal.signal(signal.SIGINT, _handle_shutdown)
signal.signal(signal.SIGTERM, _handle_shutdown)


def build_producer(bootstrap_servers, max_retries=10):
    for attempt in range(1, max_retries + 1):
        try:
            kafka_producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=5,
                retry_backoff_ms=300,
                linger_ms=5,
                batch_size=65536,
                compression_type="gzip",
                buffer_memory=33554432,
                max_block_ms=10000,
            )
            log.info("Kafka producer connected to %s", bootstrap_servers)
            return kafka_producer
        except NoBrokersAvailable:
            log.warning(
                "Kafka not ready (attempt %d/%d) - retrying in 3s...",
                attempt, max_retries,
            )
            time.sleep(3)
    raise RuntimeError(f"Could not connect to Kafka at {bootstrap_servers} after {max_retries} attempts")


def _on_send_error(exc):
    global error_count
    error_count += 1
    log.error("Kafka delivery FAILED: %s", exc)


def generate_clean_event(store=True):
    event = {
        "vehicle_id": fake.uuid4(),
        "road_id": random.choice(ROADS),
        "city_zone": random.choice(ZONES),
        "speed": random.randint(20, 100),
        "congestion_level": random.randint(1, 5),
        "weather": random.choice(WEATHER),
        "event_time": datetime.now(pytz.utc).isoformat(),
    }
    if store:
        recent_events.append(deepcopy(event))
    return event


def generate_dirty_event():
    dirty_type = random.choice(DIRTY_TYPES)
    base = generate_clean_event(store=False)

    if dirty_type == "null_speed":
        base["speed"] = None
    elif dirty_type == "negative_speed":
        base["speed"] = -abs(random.randint(10, 60))
    elif dirty_type == "extreme_speed":
        base["speed"] = random.randint(250, 600)
    elif dirty_type == "duplicate_event" and recent_events:
        base = deepcopy(random.choice(list(recent_events)))
    elif dirty_type == "late_event":
        base["event_time"] = (datetime.now(pytz.utc) - timedelta(minutes=random.randint(10, 120))).isoformat()
    elif dirty_type == "future_event":
        base["event_time"] = (datetime.now(pytz.utc) + timedelta(minutes=random.randint(5, 60))).isoformat()
    elif dirty_type == "wrong_datatype":
        base["speed"] = random.choice(["FAST", "SLOW", "UNKNOWN"])
    elif dirty_type == "schema_drift":
        base["road_condition"] = random.choice(["GOOD", "BAD", "UNDER_CONSTRUCTION"])
    elif dirty_type == "corrupt_json":
        return "###CORRUPTED_EVENT###"

    return base


def run(topic, dirty_ratio, min_delay, max_delay, bootstrap_servers):
    global sent_count, shutdown_flag, producer

    producer = build_producer(bootstrap_servers)
    log.info(
        "Producing to topic='%s' | bootstrap=%s | dirty_ratio=%.0f%% | rate=%.2f-%.2fs/msg",
        topic, bootstrap_servers, dirty_ratio * 100, min_delay, max_delay,
    )

    while not shutdown_flag:
        try:
            event = generate_clean_event() if random.random() < (1 - dirty_ratio) else generate_dirty_event()

            if isinstance(event, str):
                payload = {"raw": event, "corrupted": True}
                producer.send(topic, value=payload).add_errback(_on_send_error)
            else:
                key = event.get("city_zone", "UNKNOWN")
                producer.send(topic, key=key, value=event).add_errback(_on_send_error)

            sent_count += 1
            if sent_count % 50 == 0:
                log.info("Sent %d events | Errors: %d | Cache size: %d", sent_count, error_count, len(recent_events))
        except KafkaError as exc:
            log.error("Send error (non-fatal): %s", exc)

        time.sleep(random.uniform(min_delay, max_delay))

    log.info("Flushing in-flight messages...")
    producer.flush(timeout=15)
    producer.close()
    log.info("Producer shut down cleanly. Total sent: %d | Errors: %d", sent_count, error_count)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Traffic Data Kafka Producer")
    parser.add_argument("--topic", default=DEFAULT_TOPIC, help="Kafka topic name")
    parser.add_argument("--bootstrap", default=DEFAULT_BOOTSTRAP, help="Kafka bootstrap servers")
    parser.add_argument("--dirty-ratio", type=float, default=0.30, help="Fraction of dirty events (0-1)")
    parser.add_argument("--rate", type=float, default=None, help="Fixed send interval in seconds")
    parser.add_argument("--min-delay", type=float, default=0.5)
    parser.add_argument("--max-delay", type=float, default=1.5)
    parser.add_argument("--seed", type=int, default=None, help="Optional random seed for reproducible demos")
    args = parser.parse_args()

    if args.seed is not None:
        random.seed(args.seed)
        Faker.seed(args.seed)

    if not 0 <= args.dirty_ratio <= 1:
        raise SystemExit("--dirty-ratio must be between 0 and 1")

    if args.rate is not None:
        if args.rate <= 0:
            raise SystemExit("--rate must be > 0")
        args.min_delay = args.max_delay = args.rate

    if args.min_delay <= 0 or args.max_delay <= 0 or args.min_delay > args.max_delay:
        raise SystemExit("Delay values must be > 0 and min-delay must be <= max-delay")

    run(
        topic=args.topic,
        dirty_ratio=args.dirty_ratio,
        min_delay=args.min_delay,
        max_delay=args.max_delay,
        bootstrap_servers=args.bootstrap,
    )
