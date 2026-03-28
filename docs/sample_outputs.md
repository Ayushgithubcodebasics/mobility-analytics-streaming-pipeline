# Sample outputs and sanity checks

This file shows the kinds of outputs the project should produce after a successful local run.

## 1. Kafka topic

The producer writes JSON events to `traffic-topic`.

Example payload:

```json
{
  "vehicle_id": "VH-10027",
  "road_id": "R100",
  "city_zone": "CBD",
  "speed": "64",
  "congestion_level": 3,
  "weather": "CLEAR",
  "event_time": "2026-03-24T18:21:04.118+00:00"
}
```

## 2. Silver rejects table

Expected reject reasons include:
- `CORRUPT_JSON`
- `MISSING_VEHICLE`
- `MISSING_TIME`
- `INVALID_SPEED`
- `INVALID_TIME`
- `DUPLICATE_EVENT`

Quick SQL check:

```sql
SELECT reject_reason, COUNT(*)
FROM traffic_rejects
GROUP BY reject_reason
ORDER BY COUNT(*) DESC;
```

## 3. Gold fact table

Expected columns include:
- `vehicle_id`
- `road_id`
- `city_zone`
- `speed`
- `congestion_level`
- `event_ts`
- `peak_flag`
- `speed_band`
- `weather`
- `event_date`
- `hour`
- `ingested_at`

Quick SQL check:

```sql
SELECT COUNT(*) AS fact_rows FROM fact_traffic;
SELECT city_zone, AVG(speed) AS avg_speed, AVG(congestion_level) AS avg_congestion
FROM fact_traffic
GROUP BY city_zone;
```

## 4. Dashboard sanity checks

Expected dashboard views:
- `v_live_summary`
- `v_hourly_congestion`
- `v_road_performance`
- `v_weather_impact`
- `v_speed_distribution`
- `v_zone_kpis`

Useful checks:

```sql
SHOW VIEWS IN mobility;
SELECT * FROM v_live_summary;
SELECT * FROM v_hourly_congestion LIMIT 10;
```

## 5. Recommended proof to capture for the repo

If you want to make the GitHub project stronger, add screenshots for:
- Kafka UI topic overview
- Spark UI job page
- Power BI dashboard
- output of `SHOW VIEWS IN mobility`
