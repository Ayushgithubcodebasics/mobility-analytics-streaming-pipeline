import os
import sys
from copy import deepcopy

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "producer"))

import traffic_dirty_producer as producer


def test_generate_clean_event_has_expected_shape():
    event = producer.generate_clean_event(store=False)
    assert set(event.keys()) == {
        "vehicle_id",
        "road_id",
        "city_zone",
        "speed",
        "congestion_level",
        "weather",
        "event_time",
    }


def test_duplicate_event_reuses_cached_identity_and_timestamp(monkeypatch):
    cached = {
        "vehicle_id": "veh-123",
        "road_id": "R100",
        "city_zone": "CBD",
        "speed": 55,
        "congestion_level": 3,
        "weather": "CLEAR",
        "event_time": "2026-03-24T10:00:00+00:00",
    }
    producer.recent_events.clear()
    producer.recent_events.append(deepcopy(cached))

    def fake_choice(options):
        if options == producer.DIRTY_TYPES:
            return "duplicate_event"
        if (
            isinstance(options, list)
            and options
            and isinstance(options[0], dict)
            and options[0].get("vehicle_id") == "veh-123"
        ):
            return options[0]
        return options[0]

    monkeypatch.setattr(producer.random, "choice", fake_choice)
    duplicate = producer.generate_dirty_event()

    assert duplicate["vehicle_id"] == cached["vehicle_id"]
    assert duplicate["event_time"] == cached["event_time"]


def test_corrupt_json_dirty_type_returns_marker(monkeypatch):
    monkeypatch.setattr(
        producer.random,
        "choice",
        lambda options: "corrupt_json" if options == producer.DIRTY_TYPES else options[0],
    )
    assert producer.generate_dirty_event() == "###CORRUPTED_EVENT###"
